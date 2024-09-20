import time
import logging
from web3 import Web3
from web3.exceptions import ContractLogicError, TransactionNotFound
from eth_account import Account
from config import CONFIG
from model import DataHandler


# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class Funds:
    """
    The `Funds` class provides functionality for managing and transferring USDT (Tether) tokens on the Polygon network. 
    Specifically, it facilitates the transfer of USDT from deposit accounts to a central collection account.

    The class interacts with the Ethereum blockchain using Web3.py and utilizes a database handler to manage private 
    keys and transaction records.

    Nested Classes:
        USDT: A nested class that handles the USDT token transfer process.

    class USDT:
    """    

    class USDT:
        """
        The `USDT` class is responsible for transferring USDT tokens from a deposit account to a central collection account 
        on the Polygon network. It includes methods for fetching the current gas price, estimating gas for transactions, 
        and executing the transfer of USDT.

        Attributes:
            polygon_url (str): The URL of the Polygon network RPC endpoint.
            web3 (Web3): An instance of the Web3 class used to interact with the Ethereum blockchain.
            database (DataHandler): An instance of the DataHandler class used for database operations.
        
        Methods:
            __init__(): Initializes the USDT class, sets up the Web3 connection, and configures the database handler.
            
            get_gas_price(): Fetches the current gas price from the Ethereum network and adjusts it based on a configured percentage increase.
            
            estimate_gas(from_address, to_address, amount, usdt_contract): Estimates the gas required for a USDT transfer transaction.
            
            transfer(from_address, amount, deposit_tx_id): Executes a USDT transfer from the specified deposit account to the central collection account. 
            The method handles transaction signing, submission to the blockchain, and receipt verification.

        Example Usage:
            funds = Funds.USDT()
            funds.transfer("0xYourDepositAddress", 100, "deposit_transaction_id")
        """

        def __init__(self) -> None:
            # Configure your Web3 provider (Infura or Alchemy)
            self.polygon_url = CONFIG.API.INFURA_API_URL + CONFIG.API.INFURA_API_KEY
            self.web3 = Web3(Web3.HTTPProvider(self.polygon_url))
            self.database = DataHandler()

        # Function to get current gas price
        def get_gas_price(self):
            current_gas_price = int(self.web3.eth.gas_price * (1 + CONFIG.ETHPOLYGON.INCREASE_GAS_PRICE_PERCENTAGE / 100))
            return current_gas_price

        # Function to estimate gas for the transaction
        def estimate_gas(self, from_address, to_address, amount, usdt_contract):
            transaction = usdt_contract.functions.transfer(to_address, int(amount)).build_transaction({
                'chainId': 137,  # polygon mainnet chain id
                'gas': 69005,    # This is a placeholder and will be overridden by the estimate
                'gasPrice': self.get_gas_price(),
                'nonce': self.web3.eth.get_transaction_count(from_address),
                'from': from_address,
            })
            return self.web3.eth.estimate_gas(transaction)


        def transfer(self, from_address, amount, deposit_tx_id):
            t_amount = int(amount * (10 ** 6))  # 0.1 USDT in Wei

            # Fetch the private key
            priv_key = self.database.get_deposit_address_private_key(from_address)[0]
            private_key = priv_key['get_deposit_address_private_key']
            
            #account = Account.from_key(private_key)

            # get wallet address where the funds need to be sent to from database
            raw_to_address = self.database.get_centraladdress()[0]
            to_address = raw_to_address['depositaddress']

            # USDT contract address on Polygon
            usdt_address = CONFIG.ETHPOLYGON.USDT_CONTRACT
            usdt_contract = self.web3.eth.contract(address=usdt_address, abi=[
                {
                    "constant": False,
                    "inputs": [
                        {
                            "name": "_to",
                            "type": "address"
                        },
                        {
                            "name": "_value",
                            "type": "uint256"
                        }
                    ],
                    "name": "transfer",
                    "outputs": [
                        {
                            "name": "",
                            "type": "bool"
                        }
                    ],
                    "payable": False,
                    "stateMutability": "nonpayable",
                    "type": "function"
                }
            ])


            # Get the current gas price
            current_gas_price = self.get_gas_price()
            logger.info(f'Current gas price: {self.web3.from_wei(current_gas_price, "gwei")} Gwei')

            # Estimate gas for the transaction
            estimated_gas = self.estimate_gas(from_address, to_address, t_amount, usdt_contract)
            logger.info(f'Estimated gas: {estimated_gas}')

            # Create a transaction dictionary
            transaction = usdt_contract.functions.transfer(to_address, t_amount).build_transaction({
                'chainId': 137,  # polygon mainnet chain id
                'gas': estimated_gas,  # Use the estimated gas
                'gasPrice': current_gas_price,  # Use fetched gas price
                'nonce': self.web3.eth.get_transaction_count(from_address),
            })

            # Sign the transaction with the private key
            signed_transaction = self.web3.eth.account.sign_transaction(transaction, private_key)

            # Send the transaction to the Polygon network
            try:
                tx_hash = self.web3.eth.send_raw_transaction(signed_transaction.rawTransaction)
            except (ContractLogicError, ValueError, TransactionNotFound, Exception) as e:
                logger.error(f"Web3 Eth transaction error: {e}")
                return f"Web3 Eth transaction error: {e}"            
            tx_hash_str = tx_hash.hex()
            logger.info(f"Updating record with deposit transaction id {deposit_tx_id} to TRUE")
            self.database.update_transferred_status_true(f"'{deposit_tx_id}'")                
            logger.info(f'Transfer successfully initiated: {tx_hash_str}')
            receipt = None
            max_retries = 30
            retry_count = 0
            while receipt is None and retry_count  < max_retries:
                try:
                    receipt = self.web3.eth.get_transaction_receipt(tx_hash)
                    if receipt is not None:
                        break
                    else:
                        logger.info('Transaction not yet mined, waiting...')
                        time.sleep(10)
                except TransactionNotFound as e:
                    logger.warning(f"Waiting for receiving transaction receipt {tx_hash_str}: {e}")
                    time.sleep(10)
                except Exception as e:
                    logger.warning(f"Error while fetching transaction receipt of with TxHash {tx_hash_str}: {e}")
                    time.sleep(10)

            if receipt.status == 1:
                logger.info(f'*** RECEIPT RECEIVED: Transaction was successful. Receipt: {receipt}')
            else:
                logger.error(f'*** RECEIPT RECEIVED: Transaction failed. Receipt: {receipt}')
                #update field 'transferred' in depositlogs to FALSE to mark record to 'not processed'
                self.database.update_transferred_status_false(f"'{deposit_tx_id}'")                


