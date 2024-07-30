# krakenapi.py
import requests
import time
import hashlib
import hmac
import base64
import urllib.parse
from config import CONFIG

class KrakenAPI:
    BASE_URL = CONFIG.API.BASE_URL

    def __init__(self, api_key, private_key):
        self.api_key = api_key
        self.private_key = private_key

    def create_auth_headers(self, uri_path, post_data):
        nonce = str(int(time.time() * 1000))  # Current timestamp in milliseconds
        post_data['nonce'] = nonce

        # URL encode the POST data
        url_encoded_post_data = urllib.parse.urlencode(post_data)

        # Create the message to sign
        message = (nonce + url_encoded_post_data).encode()
        encoded_uri_path = uri_path.encode()

        # Create the signature
        secret_decoded = base64.b64decode(self.private_key)
        hmac_digest = hmac.new(secret_decoded, encoded_uri_path + hashlib.sha256(message).digest(), hashlib.sha512)
        signature = base64.b64encode(hmac_digest.digest()).decode()

        # Create headers
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'API-Key': self.api_key,
            'API-Sign': signature
        }

        return headers

    def call_api(self, endpoint_path, post_data):
        url = self.BASE_URL + endpoint_path
        headers = self.create_auth_headers(endpoint_path, post_data)

        try:
            response = requests.post(url, headers=headers, data=post_data)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()  # Parse JSON response
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except ValueError as e:
            print(f"Error parsing JSON: {e}")
            return None

    def generate_new_deposit_address(self, asset, method, new=False):
        endpoint_path = CONFIG.API.GET_DEPOSIT_ADRESSES_EP
        post_data = {
            'asset': asset,
            'method': method,
            'new': new
        }
        return self.call_api(endpoint_path, post_data)
    
    def get_deposit_methods(self, asset):
        endpoint_path = CONFIG.API.GET_DEPOSIT_METHODS_EP
        post_data = {
            'asset': asset
        }
        return self.call_api(endpoint_path, post_data)

    def get_recent_deposits(self, asset, method):
        endpoint_path = CONFIG.API.GET_DEPOSIT_STATUS_EP
        post_data = {
            'asset': asset,
            'method': method,
        }
        return self.call_api(endpoint_path, post_data)


# Example usage
if __name__ == '__main__':
    kraken = KrakenAPI(CONFIG.SPOT_API_KEY, CONFIG.SPOT_PRIVATE_KEY)

    asset = 'USDT'
    method = 'Tether USDT (ERC20)'

    new_address_response = kraken.generate_new_deposit_address(asset, method)
    if new_address_response:
        print("New Address Response:", new_address_response)
    else:
        print("Failed to generate a new deposit address.")

    deposit_methods = kraken.get_deposit_methods(asset)
    if deposit_methods:
        print("Deposit Methods: ", deposit_methods)
    else:
        print("Failed to get deposit methods.")

    recent_deposits = kraken.get_recent_deposits(asset, method)
    if recent_deposits:
        print("Recent Deposits: ", recent_deposits)
    else:
        print("Failed to get recent deposits.")
