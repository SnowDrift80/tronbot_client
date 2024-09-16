#config.py

class CONFIG:
    #FUTURES_PUBLIC_KEY = 'EEvdeuYlIw4UljdIlz1ZuVyAcMRzJAGMO185yisNIjNKL37/A9x+dZZg'
    #FUTURES_PRIVATE_KEY = 'pkoI4WuGl8GPe6S28sfz2fDq6jwOfV4Jsb8+DiLXnglxv5+kPQGHm45Nx0VgWNzLPgU8BiH/4TN9j5kUv6fYxaqj'
    #SPOT_API_KEY = 'pTF+vmYH8shJB1oYW/fOpZhxFOjRDOfDzyZQ9GPvDrAK/fSTp9ruTeMP'
    #SPOT_PRIVATE_KEY = 'WRAKh1+9o4wrUz65L6HCXU3PgxwUKhp0y9Q26gx1yBZLFY9omf6iIldkDPfW9mxTtn9t+7JiS6OSpj+kyMXrUw=='
    
    TELEGRAM_KEY = '7352388541:AAFBM0lqJ0JrkE738vSrhJ6RIa-n7ElR8Mg' # tron dev test bot
    ## TELEGRAM_KEY = '7482254233:AAHZqH8MnQL0O0R78aLZrmTpkyXsq2_rsVY' # test-environment test bot @trondemo_bot
    ## TELEGRAM_KEY = '7491925213:AAE0DXt_4hPwZFZsJGGEVb82zmMreL9UXFc' # production-environment test bot @tron_staging_bot
    ## TELEGRAM_KEY = '7282820014:AAHxC66U_9zFCsJlJAZF_UVK-CtriTzBPOI' # production bot - THIS ONE TO USE IN PRODUCTION @algoeagle_bot
    ASSET = 'USDT'
    METHOD = 'Tether USDT (ERC20)'
    ADMIN_CHAT_IDS = ['6916783573', '1366778530', '578856029']
    ADMIN_DEPOSIT_NOTIFICATION = True
    DEPOSIT_ADDR_VALIDITY = 150 # number of seconds the deposit address remains assigned to the chat_id
    DEPOSIT_ADDR_VALIDITY_BUFFER = 30 # buffer that reflects the time it can take until the deposit is credited to our account
    DEPOSIT_POLLING_INTERVAL = 10 # polling interval in seconds for incoming deposits
    DEPOSIT_REQUEST_STACK_INTERVAL = 10 # polling interval in seconds for request stack
    DEPOSIT_MINIMUM = 20 # If below the deposit minimum, the deposit receipt confirmation will ask the customer to top up the difference.
    MAX_DEPOSIT_ADDRESSES = 10 # maximum number of deposit addresses that can be used concurrently
    LOGO_PATH = 'assets/algoeagle_dark_logo.jpg'
    ENDPOINT_BASEURL = 'http://localhost:5001/api'
    RETURNS_BASEURL = 'http://localhost:5010/api'
    SUPPORT_CONTACT = '@AlgoEagleAdmin'
    CHAT_GROUP = '@AlgoEagle'
    
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
        RETROSPECT_BLOCKS = 35000 # how many blocks into the past to search for new transactions

    class API:
        INFURA_API_URL = 'https://polygon-mainnet.infura.io/v3/'
        INFURA_SOCKET_URL = 'wss://polygon-mainnet.infura.io/ws/v3/'
        INFURA_API_KEY = 'e02346fc16f8431aa3fb1c0aef9b7730' # test key
        # INFURA_API_KEY = '71db71cab031411db6c9bc496ef4463c' # production key
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

    class TEXTS:
        FAQ = """
FAQ - The most asked questions and answers can be found below! If you have any other questions, feel free to ask them and we will answer them as soon as possible!

<b>1. What is AlgoEagle?</b>
AlgoEagle is an automatic trading bot created hand in hand with AI algorithms, to find market trends and execute the safest and most profitable trades every minute, generating around 50% profit every month!. In order to use the bot, we have developed another separate telegram bot (@ to be announced soon) that will allow you to deposit funds into the pool and have the bot execute trades on your behalf!

<b>2. How can I use the bot?</b>
The process is very simple! Soon we will announce the @ of the telegram bot, which will allow you to make a deposit, withdraw funds, check your balance, and statistics of the bot over various periods.

<b>3. How safe is this bot?</b>
AlgoEagle is the safest trading bot on the market, offering the best returns. The bot has been tested for over 2 years before releasing it to the public, so you can be sure that your money is safe with AlgoEagle and will allow you to generate passive profits from the moment you make a deposit.

<b>4. Who is the team behind AlgoEagle?</b>
We are a team of developers and traders with extensive experience in the field. We have developed and used this bot privately for many years in order to all quit our jobs and achieve financial freedom. This bot is truly the best opportunity there is right now to generate wealth and forget about your jobs once your deposit grows exponentially.

<b>5. What will be the minimum deposit to use the bot?</b>
We have decided to set the deposit at just 20$. This way anyone can try out using the bot with just a small investment before committing a bigger amount.

<b>6. Are there any fees?</b>
There are only 2 types of fees that we take to support the development of the bot and cover any expenses that we have. We take 10% of the deposit and 10% of the withdrawals as a fee. Thats it. No other fees.

<b>7. How much should I invest?</b>
Its up to you! The minimum deposit is 20$, however it depends on you how much you want to invest and generate passively. With an average return of 50% every month and a deposit of 1000$, you will generate 500$ just the first month, which will be compounded every month. See this as reference:
<code>Deposit 1,000$
Month 1: 1,500$
Month 2: 2,250$
Month 3: 3,375$
Month 4: 5,062$</code>
"""



    # MAIN_ACCOUNT_PRIVATE_KEY = '8f0a81b40c2c5c3282e6ee44457be8e3cd34a2dd47e586bf2ff5f2a400ccc4d6'


