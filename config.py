#config.py

class CONFIG:
    FUTURES_PUBLIC_KEY = 'EEvdeuYlIw4UljdIlz1ZuVyAcMRzJAGMO185yisNIjNKL37/A9x+dZZg'
    FUTURES_PRIVATE_KEY = 'pkoI4WuGl8GPe6S28sfz2fDq6jwOfV4Jsb8+DiLXnglxv5+kPQGHm45Nx0VgWNzLPgU8BiH/4TN9j5kUv6fYxaqj'
    SPOT_API_KEY = 'pTF+vmYH8shJB1oYW/fOpZhxFOjRDOfDzyZQ9GPvDrAK/fSTp9ruTeMP'
    SPOT_PRIVATE_KEY = 'WRAKh1+9o4wrUz65L6HCXU3PgxwUKhp0y9Q26gx1yBZLFY9omf6iIldkDPfW9mxTtn9t+7JiS6OSpj+kyMXrUw=='
    TELEGRAM_KEY = '7482254233:AAHZqH8MnQL0O0R78aLZrmTpkyXsq2_rsVY'
    ASSET = 'USDT'
    METHOD = 'Tether USD (TRC20)'
    ADMIN_CHAT_ID = '6916783573'
    #ADMIN_CHAT_ID = '1366778530' # Italian admin
    DEPOSIT_ADDR_VALIDITY = 20 # number of seconds the deposit address remains assigned to the chat_id
    DEPOSIT_ADDR_VALIDITY_BUFFER = 10 # buffer that reflects the time it can take until the deposit is credited to our account
    DEPOSIT_POLLING_INTERVAL = 5 # polling interval in seconds for incoming deposits
    DEPOSIT_REQUEST_STACK_INTERVAL = 1 # polling interval in seconds for request stack
    MAX_DEPOSIT_ADDRESSES = 2 # maximum number of deposit addresses that can be used concurrently
    LOGO_PATH = 'assets/orca_logo.jpg'
    ENDPOINT_BASEURL = 'http://localhost:5001/api'
    
    class TRON:
        CHECK_TRON_WALLET = True
        FULL_NODE = 'https://api.trongrid.io'
        SOLIDITY_NODE = 'https://api.trongrid.io'
        EVENT_SERVER = 'https://api.trongrid.io'


    class API:
        BASE_URL = "https://api.kraken.com"
        GET_DEPOSIT_ADRESSES_EP = "/0/private/DepositAddresses"
        GET_DEPOSIT_METHODS_EP = "/0/private/DepositMethods"
        GET_DEPOSIT_STATUS_EP = "/0/private/DepositStatus"

    
    class DBCONFIG:
        DBNAME = 'OrcaBase'
        USER = 'orcauser'
        PASSWORD = 'txm9272'
        HOST = 'psql15.hq.rvg'
        PORT = '5432'








# Kraken Credentials Demo Environment
# Email: so5xdny9@futures-demo.com
# Pass: xu6vantjhhruhjyfio1s

# Kraken DEMO-FUTURES API Keys:
# Public Key: EEvdeuYlIw4UljdIlz1ZuVyAcMRzJAGMO185yisNIjNKL37/A9x+dZZg
# Private Key: pkoI4WuGl8GPe6S28sfz2fDq6jwOfV4Jsb8+DiLXnglxv5+kPQGHm45Nx0VgWNzLPgU8BiH/4TN9j5kUv6fYxaqj

# Kraken Login Pro Environment:
# Email: snowdrift80@yandex.com
# Pass: hUrqg}h'P9k9tWn

# Kraken PRO SPOT API Keys:
# API Key: pTF+vmYH8shJB1oYW/fOpZhxFOjRDOfDzyZQ9GPvDrAK/fSTp9ruTeMP
# Private Key: WRAKh1+9o4wrUz65L6HCXU3PgxwUKhp0y9Q26gx1yBZLFY9omf6iIldkDPfW9mxTtn9t+7JiS6OSpj+kyMXrUw==
