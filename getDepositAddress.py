import hashlib
import hmac
import base64
import time
import requests
import json
from config import CONFIG

PUBLIC_KEY = CONFIG.SPOT_API_KEY
PRIVATE_KEY = CONFIG.SPOT_PRIVATE_KEY

def create_authent(endpoint_path, data):
    nonce = str(int(time.time() * 1000))  # Current timestamp in milliseconds
    message = (nonce + data).encode()
    sha256_hash = hashlib.sha256(message).digest()

    secret_decoded = base64.b64decode(PRIVATE_KEY)
    hmac_digest = hmac.new(secret_decoded, sha256_hash, hashlib.sha512).digest()
    authent = base64.b64encode(hmac_digest).decode()

    return authent, nonce

def generate_new_deposit_address(asset, method, new=True):
    endpoint_path = '/0/private/DepositAddresses'
    url = f'https://api.kraken.com{endpoint_path}'
    
    data = {
        'nonce': str(int(time.time() * 1000)),
        'asset': asset,
        'method': method,
        'new': new
    }
    
    data_str = '&'.join(f'{key}={value}' for key, value in data.items())
    authent, nonce = create_authent(endpoint_path, data_str)
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'API-Key': PUBLIC_KEY,
        'API-Sign': authent
    }
    
    try:
        response = requests.post(url, headers=headers, data=data_str)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()  # Parse JSON response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except ValueError as e:
        print(f"Error parsing JSON: {e}")
        return None

# Example usage
asset = 'XBT'
method = 'Bitcoin'

# Generate a new deposit address
new_address_response = generate_new_deposit_address(asset, method)
if new_address_response:
    print("New Address Response:", new_address_response)
else:
    print("Failed to generate a new deposit address.")
