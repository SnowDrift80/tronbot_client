import hashlib
import hmac
import base64
import time
import requests
from config import CONFIG

PUBLIC_KEY = CONFIG.SPOT_API_KEY
PRIVATE_KEY = CONFIG.SPOT_PRIVATE_KEY

def create_authent(uri_path, data):
    nonce = str(int(time.time() * 1000))  # Current timestamp in milliseconds
    post_data = data + f'nonce={nonce}'.encode()
    sha256_hash = hashlib.sha256(nonce.encode() + post_data).digest()
    
    secret_decoded = base64.b64decode(PRIVATE_KEY)
    hmac_digest = hmac.new(secret_decoded, uri_path.encode() + sha256_hash, hashlib.sha512).digest()
    authent = base64.b64encode(hmac_digest).decode()
    
    return authent, nonce

def get_ticker_for_symbol(symbol):
    endpoint_path = f'/0/public/Ticker'
    url = f'https://api.kraken.com{endpoint_path}?pair={symbol}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json(), url  # Parse JSON response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except ValueError as e:
        print(f"Error parsing JSON: {e}")
        return None

# Example usage for a specific symbol
symbol = 'USDTUSD'  # Example symbol for Tether (USDT) against US Dollar (USD)
ticker_data, url = get_ticker_for_symbol(symbol)
if ticker_data:
    print(ticker_data)
else:
    print(f"No data found for symbol {symbol}")

print("\nURL: ", url)
