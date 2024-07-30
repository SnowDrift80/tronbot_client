import time
from web3 import Web3
from eth_account import Account
from config import CONFIG
from model import DataHandler


class Funds:
    class USDT:
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
            print(f"t_amount = {t_amount}")

            # Fetch the private key
            priv_key = self.database.get_deposit_address_private_key(from_address)[0]
            private_key = priv_key['get_deposit_address_private_key']
            

            print(f"DECODED PRIVATE KEY: {private_key}")

            #account = Account.from_key(private_key)

            # get wallet address where the funds need to be sent to from database
            raw_to_address = self.database.get_centraladdress()[0]
            to_address = raw_to_address['depositaddress']
            print(f"transaction = usdt_contract.functions.transfer({to_address}, {t_amount}).build_transaction(")

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
            print(f'Current gas price: {self.web3.from_wei(current_gas_price, "gwei")} Gwei')

            # Estimate gas for the transaction
            estimated_gas = self.estimate_gas(from_address, to_address, t_amount, usdt_contract)
            print(f'Estimated gas: {estimated_gas}')

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
            tx_hash = self.web3.eth.send_raw_transaction(signed_transaction.rawTransaction)
            tx_hash_str = tx_hash.hex()
            print(f"\n\n\n\n\n TX_HASH_STR: {tx_hash_str}\n\n\n\n\n\n")
            print(f"updating record with deposit transaction id {deposit_tx_id} to TRUE")
            self.database.update_transferred_status_true(f"'{deposit_tx_id}'")                
            print(f'*** TRANSFER SUCCESSFULLY INITIATED: {tx_hash_str}')
            receipt = None
            while receipt is None:
                try:
                    receipt = self.web3.eth.get_transaction_receipt(tx_hash)
                    if receipt is not None:
                        break
                    else:
                        print('Transaction not yet mined, waiting...')
                        time.sleep(2)
                except Exception as e:
                    print(f"Error while fetching transaction receipt of with TxHash {tx_hash_str}: {e}")
                    time.sleep(2)

            if receipt.status == 1:
                print(f'*** RECEIPT RECEIVED: Transaction was successful. Receipt: {receipt}')
            else:
                print(f'*** RECEIPT RECEIVED: Transaction failed. Receipt: {receipt}')
                #update field 'transferred' in depositlogs to FALSE to mark record to 'not processed'
                self.database.update_transferred_status_false(f"'{deposit_tx_id}'")                


