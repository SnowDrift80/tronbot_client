"""
The class DepositLogs contains the fetch_logs() method, that will return the 
transaction logs of all wallet address passed as parameter to the __init__ constructor.
"""

import binascii
import requests
from web3 import Web3
from config import CONFIG
import datetime
from model import DataHandler



class DepositLogs:
    def __init__(self, wallet_addresses) -> None:
        # Connect to Infura WebSocket
        infura_url = CONFIG.API.INFURA_SOCKET_URL + CONFIG.API.INFURA_API_KEY
        self.web3 = Web3(Web3.WebsocketProvider(infura_url))

        # Infura RPC endpoint for direct calls
        self.rpc_url = CONFIG.API.INFURA_API_URL + CONFIG.API.INFURA_API_KEY

        # Contract details
        self.contract_address = self.web3.to_checksum_address(CONFIG.ETHPOLYGON.USDT_CONTRACT)  # USDT contract
        self.wallet_addresses = []
        for address in wallet_addresses:
            self.wallet_addresses.append(self.web3.to_checksum_address(address['deposit_address']))

        # Transfer event signature
        self.transfer_event_signature = self.web3.keccak(text="Transfer(address,address,uint256)").hex()


    def get_latest_block(self):
        """
        Fetch the latest block number from the Ethereum network.
        """
        return self.web3.eth.get_block_number()


    def get_block_by_number(self, block_number):
        """
        Fetch block details using a direct RPC call.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(block_number), False],
            "id": 1
        }
        response = requests.post(self.rpc_url, json=payload)
        result = response.json().get('result', {})
        return result


    def handle_event(self, event):
        # print("Raw data:", event['data'])
        # Decode the event data
        transaction_hash = event['transactionHash'].hex()
        block_number = event['blockNumber']
        from_address = self.web3.to_checksum_address("0x" + event['topics'][1].hex()[26:])
        to_address = self.web3.to_checksum_address("0x" + event['topics'][2].hex()[26:])
        
        # Convert bytes to hex string and then to integer
        data_hex = binascii.hexlify(event['data']).decode('utf-8')
        amount = int(data_hex, 16) / (10 ** 6)  # Adjust for USDT's decimals

        # fetch timestamp of block (only blocks have timestamp, not individual transactions inside the block)
        # block = web3.eth.get_block(block_number)
        block = self.get_block_by_number(block_number)
        timestamp = int(block.get('timestamp', 0), 16) # convert hex timestamp to int

        # Convert timestamp to human-readable format
        transaction_date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

        # Check if the transfer is to your wallet address
        if to_address in self.wallet_addresses:
            print(f"Transfer detected: {amount} USDT \nfrom.............: {from_address} \nto...............: {to_address}\ntransaction-id...: {transaction_hash}\nblock-number.....: {block_number}\nblock timestamp..: {transaction_date}\n\n")
        
        log = {
            'from_address': str(from_address),
            'to_address': str(to_address),
            'transaction_id': str(transaction_hash),
            'block_number': int(block_number),
            'block_timestamp': transaction_date,
            'amount': float(amount)
        }
        return log


    # def fetch_logs(self):
    #     # Convert wallet address to 32-byte padded hex string
    #     result = [] # final list that will contain all logs
    #     wallet_addresses_padded = []
    #     for adr in self.wallet_addresses:
    #         print(f"ADR: {adr}")
    #         adr_padded = f"0x{adr[2:].rjust(64, '0')}"  # Pad wallet_address to 32 bytes
    #         wallet_addresses_padded.append(adr_padded)

    #     database = DataHandler()
    #     for wallet_address_padded in wallet_addresses_padded:
    #         # Fetch logs for the specific block
    #         # block_number = 59667010 - that was a transaction of 2 USDT
    #         latest_block = self.get_latest_block()
    #         retrospect = CONFIG.ETHPOLYGON.RETROSPECT_BLOCKS
    #         startblock = latest_block - retrospect
    #         logs = None
    #         try:
    #             logs = self.web3.eth.get_logs({
    #                 'address': self.contract_address,
    #                 'fromBlock': startblock,
    #                 'toBlock': latest_block,
    #                 'topics': [self.transfer_event_signature, None, wallet_address_padded]  # Use padded wallet_address directly
    #             })
    #         except Exception as e:
    #             print(f"Error while fetching transaction logs: {e}")

    #         # Process the logs
    #         if logs:
    #             for log in logs:
    #                 row = self.handle_event(log)
    #                 if row:
    #                     result.append(row)

    #             database.insert_depositlogs(result) # SQL procedure inserts only new deposits into the depositlog table
    #     return result


    def fetch_logs(self):
        if not self.wallet_addresses:
            return # there's nothing to do if self.wallet_addresses == 0
        wallet_addresses_padded = []
        print(f"FETCH_LOGS: len self.wallet_addresses: {len(self.wallet_addresses)}")
        for adr in self.wallet_addresses:
            # Convert wallet addresses to 32-byte padded hex strings
            adr_padded = f"0x{adr[2:].rjust(64, '0')}"  # Pad wallet_address to 32 bytes
            wallet_addresses_padded.append(adr_padded)

        # Fetch the latest block and calculate the start block
        latest_block = self.get_latest_block()
        retrospect = CONFIG.ETHPOLYGON.RETROSPECT_BLOCKS
        startblock = latest_block - retrospect

        result = []

        # Split the padded addresses into batches
        batch_size = CONFIG.ETHPOLYGON.GET_BALANCE_BATCH_SIZE
        batches = [wallet_addresses_padded[i:i + batch_size] for i in range(0, len(wallet_addresses_padded), batch_size)]

        database = DataHandler()
        for batch in batches:
            logs = None
            try:
                # Query logs for the current batch of wallet addresses
                logs = self.web3.eth.get_logs({
                    'address': self.contract_address,
                    'fromBlock': startblock,
                    'toBlock': latest_block,
                    'topics': [self.transfer_event_signature, None, batch]  # Use batch of addresses
                })
            except Exception as e:
                print(f"Error while fetching transaction logs for batch: {e}")

            # Process the logs
            if logs:
                for log in logs:
                    row = self.handle_event(log)
                    if row:
                        result.append(row)

                # Insert logs into the database after each batch
                database.insert_depositlogs(result)  # Insert new logs into the depositlog table

        return result

