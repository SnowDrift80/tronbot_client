import logging
from time import sleep
import json
import requests
from requests.exceptions import RequestException
from config import CONFIG
from model import DataHandler

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

infura_url = CONFIG.API.INFURA_API_URL + CONFIG.API.INFURA_API_KEY
contract_address = CONFIG.ETHPOLYGON.USDT_CONTRACT  # USDT contract on Polygon
database = DataHandler()

class EthAPI:
    
    def get_recent_deposits(sel, number_of_batches):
        """
        The get_recent_deposits method retrieves the list of deposit_addresses from the database.
        Next, the list is split in batches of CONFIG.ETHPOLYGON.GET_BALANCE_BATCH_SIZE, for example batches of 10.

        This function returns a list containing a dictionary that consists of following key/value pairs:
        deposit_address: wallet_address   
        balance:         balance_amount

        The balance_amount represents the present balance on the respective wallet_address.
        """
        # Retrieve deposit addresses from the database
        deposit_addresses = database.get_depositaddresses()
        wallet_addresses = [address['depositaddress'] for address in deposit_addresses]

        batches = []
        row = []
        for i, adr in enumerate(wallet_addresses):
            if i % CONFIG.ETHPOLYGON.GET_BALANCE_BATCH_SIZE  == 0 and i != 0:
                batches.append(row)
                row = []
            row.append(adr)

        if len(row):
            batches.append(row)


        max_batches = len(batches) if number_of_batches > len(batches) else number_of_batches
        batches = batches[: max_batches] # this solution is to support dynamic size of number of batches in order to manage API access rate
        logger.info(f"len(batches): {len(batches)}   number_of_batches: {number_of_batches}    max_batches: {max_batches}")
        results = []

        print(f"NUMBER OF BATCHES: {len(batches)}")
        for batch in batches:
            # Create batch request data for balance queries
            balance_requests = [
                {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [
                        {
                            "to": contract_address,
                            "data": f"{CONFIG.ETHPOLYGON.BALANCEOF_FUNCTION}000000000000000000000000{wallet_address[2:]}"
                        },
                        "latest"
                    ],
                    "id": index
                }
                for index, wallet_address in enumerate(batch)
            ]

            try:            
                # Send batch request to Infura for balances
                balance_response = requests.post(
                    infura_url, 
                    data=json.dumps(balance_requests), 
                    headers={'Content-Type': 'application/json'}
                )
                balance_response.raise_for_status()
                balance_responses = balance_response.json()
                sleep(1)

                # Process balance responses
                if balance_responses:
                    for i, balance_result in enumerate(balance_responses):
                        if 'result' in balance_result:
                            # proceed if 'result' key exists - this is an intermittent issue with Infura, still being investigated
                            wallet_address = batch[balance_result['id']] # the wallet address is in the batch[index]
                            balance_hex = balance_result['result']
                            balance_amount = int(balance_hex, 16) / (10 ** 6)  # Convert balance to USDT

                            results.append({
                                'deposit_address': wallet_address,
                                'balance': balance_amount
                            })
                        else:
                            # handle case where 'result' is missing
                            logger.warning(f"Infura request balances batch, no 'result' key in returned message: {balance_result}")
            except RequestException as e:
               logger.error(f"Error fetching balances for batch {batch}: {str(e)}")

        return results
