#config.py

class CONFIG:
    FUTURES_PUBLIC_KEY = 'EEvdeuYlIw4UljdIlz1ZuVyAcMRzJAGMO185yisNIjNKL37/A9x+dZZg'
    FUTURES_PRIVATE_KEY = 'pkoI4WuGl8GPe6S28sfz2fDq6jwOfV4Jsb8+DiLXnglxv5+kPQGHm45Nx0VgWNzLPgU8BiH/4TN9j5kUv6fYxaqj'
    SPOT_API_KEY = 'pTF+vmYH8shJB1oYW/fOpZhxFOjRDOfDzyZQ9GPvDrAK/fSTp9ruTeMP'
    SPOT_PRIVATE_KEY = 'WRAKh1+9o4wrUz65L6HCXU3PgxwUKhp0y9Q26gx1yBZLFY9omf6iIldkDPfW9mxTtn9t+7JiS6OSpj+kyMXrUw=='
    TELEGRAM_KEY = '7482254233:AAHZqH8MnQL0O0R78aLZrmTpkyXsq2_rsVY'
    ASSET = 'USDT'
    METHOD = 'Tether USDT (ERC20)'
    ADMIN_CHAT_ID = '6916783573'
    #ADMIN_CHAT_ID = '1366778530' # Italian admin
    DEPOSIT_ADDR_VALIDITY = 150 # number of seconds the deposit address remains assigned to the chat_id
    DEPOSIT_ADDR_VALIDITY_BUFFER = 30 # buffer that reflects the time it can take until the deposit is credited to our account
    DEPOSIT_POLLING_INTERVAL = 60 # polling interval in seconds for incoming deposits
    DEPOSIT_REQUEST_STACK_INTERVAL = 10 # polling interval in seconds for request stack
    MAX_DEPOSIT_ADDRESSES = 10 # maximum number of deposit addresses that can be used concurrently
    LOGO_PATH = 'assets/orca_logo.jpg'
    ENDPOINT_BASEURL = 'http://localhost:5001/api'
    
    class TRON:
        CHECK_TRON_WALLET = True
        FULL_NODE = 'https://api.trongrid.io'
        SOLIDITY_NODE = 'https://api.trongrid.io'
        EVENT_SERVER = 'https://api.trongrid.io'

    class ETHPOLYGON:
        USDT_CONTRACT = '0xc2132D05D31c914a87C6611C10748AEb04B58e8F'
        BACKUP_CENTRAL_ADDRESS =  '0x92ed6e3488C3722225FC7a3276436e0F55c7194b' # is used if db request get_central address returns null
        BALANCEOF_FUNCTION = '0x70a08231'
        GET_BALANCE_BATCH_SIZE = 5 # currently infura supports a maximum batch size of 9
        INCREASE_GAS_PRICE_PERCENTAGE = 10 # 20% is aggressive, 10% often enough to get prioritized transaction


    class API:
        INFURA_API_URL = 'https://polygon-mainnet.infura.io/v3/'
        INFURA_SOCKET_URL = 'wss://polygon-mainnet.infura.io/ws/v3/'
        INFURA_API_KEY = 'e02346fc16f8431aa3fb1c0aef9b7730'
        BASE_URL = "https://api.kraken.com"
        GET_DEPOSIT_ADRESSES_EP = "/0/private/DepositAddresses"
        GET_DEPOSIT_METHODS_EP = "/0/private/DepositMethods"
        GET_DEPOSIT_STATUS_EP = "/0/private/DepositStatus"

    
    class DBCONFIG:
        DBNAME = 'OrcaClient'
        USER = 'orcauser'
        PASSWORD = 'txm9272'
        HOST = 'psql15.hq.rvg'
        PORT = '5432'

    class RETURNS_API:
        APPSERVER_URL = "http://localhost:5010"
        GET_BALANCE = "/api/get_balance/"
        GET_FACTOR = "/api/get_factor/"
        HANDLE_DEPOSIT = "/api/handle_deposit/"
        GET_CLIENT = "/api/get_client/"
        WITHDRAW = "/api/withdraw/"
        ROLLBACK_WITHDRAWAL = "/api/rollback_withdrawal"

    class FEES:
        DEPOSIT_FEE = 10 # deposit fee in percent to be deducted from the deposit
        WITHDRAWAL_FEE = 10 # withdrawal fee in percent to be deducted from the deposit



    MAIN_ACCOUNT_PRIVATE_KEY = '8f0a81b40c2c5c3282e6ee44457be8e3cd34a2dd47e586bf2ff5f2a400ccc4d6'




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
