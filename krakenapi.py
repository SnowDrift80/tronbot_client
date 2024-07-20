# krakenapi.py
import requests
import time
import hashlib
import hmac
import base64
import urllib.parse
import logging
from config import CONFIG

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KrakenAPI:
    BASE_URL = CONFIG.API.BASE_URL

    def __init__(self, api_key, private_key):
        """
        Initialize KrakenAPI instance with API key and private key.

        Args:
            api_key (str): The API key for authentication.
            private_key (str): The private key for signing requests.
        """
        self.api_key = api_key
        self.private_key = private_key

    def create_auth_headers(self, uri_path, post_data):
        """
        Create authentication headers for API requests.

        Args:
            uri_path (str): The URI path for the API endpoint.
            post_data (dict): The POST data to be sent.

        Returns:
            dict: Headers dictionary with authentication information.
        """
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
        """
        Make a POST request to the Kraken API endpoint.

        Args:
            endpoint_path (str): The endpoint path for the API request.
            post_data (dict): The POST data to be sent.

        Returns:
            dict or None: JSON response from the API if successful, None on error.
        """
        url = self.BASE_URL + endpoint_path
        headers = self.create_auth_headers(endpoint_path, post_data)

        try:
            response = requests.post(url, headers=headers, data=post_data)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()  # Parse JSON response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Kraken API: {e}")
            return None
        except ValueError as e:
            logger.error(f"Error parsing JSON response from Kraken API: {e}")
            return None

    def generate_new_deposit_address(self, asset, method, new=False):
        """
        Generate a new deposit address for the specified asset and method.

        Args:
            asset (str): The asset symbol (e.g., 'BTC', 'ETH').
            method (str): The deposit method (e.g., 'crypto', 'fiat').
            new (bool): Whether to generate a new address or retrieve an existing one.

        Returns:
            dict or None: JSON response from the API if successful, None on error.
        """
        endpoint_path = CONFIG.API.GET_DEPOSIT_ADRESSES_EP
        post_data = {
            'asset': asset,
            'method': method,
            'new': new
        }
        return self.call_api(endpoint_path, post_data)
    
    def get_deposit_methods(self, asset):
        """
        Retrieve deposit methods for the specified asset.

        Args:
            asset (str): The asset symbol (e.g., 'BTC', 'ETH').

        Returns:
            dict or None: JSON response from the API if successful, None on error.
        """
        endpoint_path = CONFIG.API.GET_DEPOSIT_METHODS_EP
        post_data = {
            'asset': asset
        }
        return self.call_api(endpoint_path, post_data)

    def get_recent_deposits(self, asset, method):
        """
        Retrieve recent deposits for the specified asset and method.

        Args:
            asset (str): The asset symbol (e.g., 'BTC', 'ETH').
            method (str): The deposit method (e.g., 'crypto', 'fiat').

        Returns:
            dict or None: JSON response from the API if successful, None on error.
        """
        endpoint_path = CONFIG.API.GET_DEPOSIT_STATUS_EP
        post_data = {
            'asset': asset,
            'method': method,
        }
        return self.call_api(endpoint_path, post_data)

