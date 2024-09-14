import re
import json
import base58
import logging
import asyncio
import uvicorn
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor
# from tronapi import Tron
from decimal import Decimal, ROUND_HALF_UP
from fastapi_app.fastapi_app import app  # Import the FastAPI app
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, Bot
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, CallbackContext, ConversationHandler
from datetime import datetime
from config import CONFIG
from model import DataHandler
from ethapi import EthAPI
from eth_utils import is_checksum_address, to_checksum_address, is_address
from client import Client
from depositstack import DepositStack
from withdraw_data import ClientWithdrawal
from bot_workflows import Workflows
from deposit_logs import DepositLogs
from transfer import Funds
from telegram_bot import application

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)

# initiate logger
logger = logging.getLogger(__name__)

# global flag for shutdown signal
# shutdown_event = asyncio.Event()
executor = ThreadPoolExecutor(max_workers=3)


# Initialize global variables
active_chats = []

try:
    database = DataHandler()
except Exception as e:
    logger.error(f"Failed to initialize DataHandler: {e}")
    raise

try:
    depositstack = DepositStack(database)
except Exception as e:
    logger.error(f"Failed to initialize DepositStack: {e}")
    raise

try:
    withdrawals = ClientWithdrawal()
except Exception as e:
    logger.error(f"Failed to initialize ClientWithdrawal: {e}")
    raise


async def validate_address(address, chat_id):
    """
    Validate cryptocurrency wallet address.
    Args: 
        address (str): The wallet address to validate.
        chat_id (int): The chat ID to send messages to.
    Returns: 
        bool: True if valid, False otherwise.
    """
    # Check if address is Ethereum (0x... format) or Tron (T... format)
    if address.startswith('0x'):
        return await validate_ethereum_address(address, chat_id)
    elif address.startswith('T'):
        return await validate_tron_address(address, chat_id)
    else:
        await depositstack.bot_message(chat_id=chat_id, message="<code>Invalid address format</code>")
        return False

def has_upper_and_lower_letters(s):
    has_upper = any(char.isupper() for char in s) # checks for upppercase letters
    has_lower = any(char.islower() for char in s) # checks for lowercase letters
    return has_upper and has_lower # logical 'and' expression is true if both values are true


# Validate Ethereum address with checksum and network check
async def validate_ethereum_address(address, chat_id):
    """
    Validate Ethereum address with checksum and network existence.
    Args:
        address (str): The Ethereum wallet address.
        chat_id (int): The chat ID to send messages to.
    Returns:
        bool: True if valid, False otherwise.
    """
    check_result = True

    try:
        # Check address format
        if not re.match(r'^0x[a-fA-F0-9]{40}$', address):
            message = f"<code>checking wallet format....... 🚫</code>"
            await depositstack.bot_message(chat_id=chat_id, message=message)
            return False
        else:
            message = f"<code>checking wallet format....... ✅</code>"
            await depositstack.bot_message(chat_id=chat_id, message=message)

        # Checksum check
        if has_upper_and_lower_letters(address): # only addresses with mixed letters have checksum!
            try:
                if not is_checksum_address(address):
                    address = to_checksum_address(address)
                    message = f"<code>testing wallet checksum...... 🚫</code>"
                    await depositstack.bot_message(chat_id=chat_id, message=message)
                    return False
                else:
                    message = f"<code>testing wallet checksum...... ✅</code>"
                    await depositstack.bot_message(chat_id=chat_id, message=message)
            except Exception as e:
                logger.error(f"Checksum validation error: {e}")
                message = f"<code>testing wallet checksum...... 🚫</code>"
                await depositstack.bot_message(chat_id=chat_id, message=message)
                return False

    except re.error as regex_error:
        logger.error(f"Regex error while validating address: {regex_error}")
        check_result = False
        await depositstack.bot_message(chat_id=chat_id, message="<code>Error validating address format</code>")

    except Exception as general_error:
        logger.error(f"General error during address validation: {general_error}")
        check_result = False
        await depositstack.bot_message(chat_id=chat_id, message="<code>An unexpected error occurred</code>")

    return check_result


# validate USDT ERC20 wallet address
async def validate_tron_address(address, chat_id):
    """
    Validate USDT wallet address in ERC20 network.
    Args: 
        address (str): The wallet address to validate.
        chat_id (int): The chat ID to send messages to.
    Returns: 
        bool: True if valid, False otherwise.
    """
    check_result = True
    
    try:
        # Check address format
        if not re.match(r'^T[A-Za-z0-9]{33}$', address):
            check_result = False
            message = f"<code>checking wallet format....... 🚫</code>"
            await depositstack.bot_message(chat_id=chat_id, message=message)
            return check_result
        else:
            message = f"<code>checking wallet format....... ✅</code>"
            await depositstack.bot_message(chat_id=chat_id, message=message)

        # Checksum check
        address_bytes = base58.b58decode(address)
        checksum = address_bytes[-4:]
        calculated_checksum = hashlib.sha256(hashlib.sha256(address_bytes[:-4]).digest()).digest()[:4]
        if checksum != calculated_checksum:
            check_result = False
            message = f"<code>testing wallet checksum...... 🚫</code>"
            await depositstack.bot_message(chat_id=chat_id, message=message)
            return check_result
        else:
            message = f"<code>testing wallet checksum...... ✅</code>"
            await depositstack.bot_message(chat_id=chat_id, message=message)
        
        # Optional check if address exists in Tron network
        if CONFIG.TRON.CHECK_TRON_WALLET:
            try:
                full_node = CONFIG.TRON.FULL_NODE
                solidity_node = CONFIG.TRON.SOLIDITY_NODE
                event_server = CONFIG.TRON.EVENT_SERVER

                # tron = Tron(full_node=full_node, solidity_node=solidity_node, event_server=event_server)

                # Try to get the latest block to verify connection
                # latest_block = tron.trx.get_block('latest')
                logger.info("Connection to Tron network successful.")

                # account_info = tron.trx.get_account(address)
                message = f"<code>validating in TRON network... ✅</code>"
                await depositstack.bot_message(chat_id=chat_id, message=message)

            except Exception as e:
                logger.error(f"TRON Network Check Error: {e}")
                check_result = False
                message = f"<code>validating in TRON network... 🚫</code>"
                await depositstack.bot_message(chat_id=chat_id, message=message)
                return check_result

    except re.error as regex_error:
        logger.error(f"Regex error while validating address: {regex_error}")
        check_result = False
        await depositstack.bot_message(chat_id=chat_id, message="<code>Error validating address format</code>")

    except base58.ValueError as base58_error:
        logger.error(f"Base58 decoding error: {base58_error}")
        check_result = False
        await depositstack.bot_message(chat_id=chat_id, message="<code>Error decoding address checksum</code>")

    except Exception as general_error:
        logger.error(f"General error during address validation: {general_error}")
        check_result = False
        await depositstack.bot_message(chat_id=chat_id, message="<code>An unexpected error occurred</code>")

    return check_result


# Command handlers
async def start(update: Update, context: CallbackContext) -> None:
    """
    Handles the /start command for the Telegram bot.

    Args:
        update (Update): The update object representing an incoming update.
        context (CallbackContext): The context object containing callback data.
    """
    try:
        await update.message.reply_text('Great ! All is ready to start.\n\nBefore using our service, we strongly recommend you to carefully review the functionality of each trading bot button.')
        await startmenu(update, context)
    except Exception as e:
        logger.error(f"Error in start handler: {e}")
        await update.message.reply_text('An error occurred while processing your request. Please try again later.')


async def startmenu(update: Update, context: CallbackContext) -> None:
    """
    Displays the start menu to the user with options for Deposit, Balance, and Withdraw.

    Args:
        update (Update): The update object representing an incoming update.
        context (CallbackContext): The context object containing callback data.
    """
    try:
        user = update.effective_user
        img = CONFIG.LOGO_PATH
        message = (
            f"<b>Hello {user.first_name}!\nWelcome to AlgoEagle.</b>\n\n"
            "Please choose an option from the menu below:\n\n"
            "<b>Deposit</b>:\n<code>Add funds to your account.</code>\n\n"
            "<b>Balance</b>:\n<code>Check your current balance.</code>\n\n"
            "<b>Withdraw</b>:\n<code>Withdraw funds from your account.</code>\n\n"
            "<b>AlgoEagle Chat</b>:\n<code>Join our community chat.</code>\n\n"
            "<b>Statistics</b>:\n<code>View your trading statistics.</code>\n\n"
            "<b>FAQ</b>:\n<code>Frequently asked questions.</code>\n\n"
            "<b>Support</b>:\n<code>Get help from our support team.</code>"
        )
        logger.info(f"Displaying start menu to user: {user.id}")

        keyboard = [
            [InlineKeyboardButton("Deposit\u2003\u2003\u2003\u2003💳", callback_data=json.dumps(
                {
                    "status": "request_deposit",
                    "decision": "",
                })),
            InlineKeyboardButton("Balance\u2003\u2003\u2003🏦", callback_data=json.dumps(
                {
                    "status": "get_balance",
                    "decision": "",
                }))],
            [InlineKeyboardButton("Withdraw\u2003💰", callback_data=json.dumps(
                {
                    "status": "request_withdraw",
                    "decision": "",
                })),
            InlineKeyboardButton("AlgoEagle Chat\u2003💬", callback_data=json.dumps(
                {
                    "status": Workflows.GotoChat.GOC_0['function'],
                    "decision": "",
                }))],
            [InlineKeyboardButton("Statistics\u2003📈", callback_data=json.dumps(
                {
                    "status": Workflows.GetStatistics.GES_0['function'],
                    "decision": "",
                })),
            InlineKeyboardButton("FAQ\u2003ℹ️", callback_data=json.dumps(
                {
                    "status": Workflows.GotoFAQ.GOF_0['function'],
                    "decision": "",
                }))],
            [InlineKeyboardButton("Support\u2003💁‍♂️", callback_data=json.dumps(
                {
                    "status": Workflows.ContactSupport.COS_0['function'],
                    "decision": "",
                }))],
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        with open(img, 'rb') as imgt:
            await update.message.reply_photo(photo=img, caption=message, parse_mode='HTML', reply_markup=reply_markup)
    except FileNotFoundError:
        error_message = "Logo image file not found. Please check the LOGO_PATH in the configuration."
        logger.error(error_message)
        await update.message.reply_text(error_message)
    except Exception as e:
        logger.error(f"Error in startmenu handler: {e}")
        await update.message.reply_text('An error occurred while displaying the start menu. Please try again later.')


async def enter_address(update: Update, context: CallbackContext) -> int:
    """
    Handles the user input for entering a wallet address and sends a withdrawal request to the admin.

    Args:
        update (Update): The update object representing an incoming update.
        context (CallbackContext): The context object containing callback data.

    Returns:
        int: The end of the conversation handler.
    """
    try:
        user_data = context.user_data
        user_data['address'] = update.message.text

        user = update.message.from_user
        amount = user_data.get('amount')
        address = user_data.get('address')

        if not amount or not address:
            logger.warning(f"Missing amount or address in user data for user: {user.id}")
            await update.message.reply_text("There was an error processing your request. Please try again.")
            user_data.clear()
            return ConversationHandler.END

        admin_message = f'User {user.username} requested withdrawal of {amount} TRX to address {address}.'
        logger.info(f"Sending withdrawal request for user {user.id} to admin.")
        for admin_chat_id in CONFIG.ADMIN_CHAT_IDS:
            await context.bot.send_message(chat_id=admin_chat_id, text=admin_message)
        await update.message.reply_text(f'Your withdrawal request has been sent to the admin.')

        user_data.clear()
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Error in enter_address handler for user {user.id}: {e}")
        await update.message.reply_text('An error occurred while processing your request. Please try again later.')
        user_data.clear()
        return ConversationHandler.END


async def help(update: Update, context: CallbackContext) -> None:
    """
    Sends a list of bot commands to the user.

    Args:
        update (Update): The update object representing an incoming update.
        context (CallbackContext): The context object containing callback data.
    """
    try:
        bot_commands = (
            "<b><u>List of bot commands:</u></b>\n\n"
            "🚀  /start:\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003Starts the bot\n\n"
            "💳  /deposit:\u2003\u2003\u2003\u2003\u2003Make deposit\n\n"
            "🏦  /balance:\u2003\u2003\u2003\u2003\u2003Your account balance\n\n"
            "💰  /withdraw:\u2003\u2003Request withdraw\n\n"
            "❓  /help:\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003Displays this help\n\n"
        )

        await update.message.reply_text(bot_commands, parse_mode='HTML')
        logger.info(f"Help command executed by user {update.effective_user.id}")

    except Exception as e:
        logger.error(f"Error in help command for user {update.effective_user.id}: {e}")
        await update.message.reply_text('An error occurred while processing your request. Please try again later.')


async def handle_message(update: Update, context: CallbackContext) -> None:
    """
    Handles incoming text messages from users. Replies with the same text if no 'amount' or 'address' is found in user data.

    Args:
        update (Update): The update object representing an incoming update.
        context (CallbackContext): The context object containing callback data.
    """
    try:
        # Check if 'amount' and 'address' are in user_data
        if context.user_data.get('amount') is None or context.user_data.get('address') is None:
            text = update.message.text
            await update.message.reply_text(f'You said: {text}')
            logger.info(f"Message from user {update.effective_user.id}: {text}")
        else:
            logger.warning(f"Unexpected state: user {update.effective_user.id} has 'amount' or 'address' in user_data.")
    except Exception as e:
        logger.error(f"Error handling message from user {update.effective_user.id}: {e}")
        await update.message.reply_text('An error occurred while processing your message. Please try again later.')


def add_chat(update: Update, chat_id):
    """
    Adds a new chat to the active_chats list.

    Args:
        update (Update): The update object representing an incoming update.
        chat_id (int): The chat ID of the new chat.

    Returns:
        dict: The new Telegram user object added to the active_chats list.
    """
    try:
        # Extract user information from the update
        chat_id = update.message.chat_id
        firstname = update.effective_user.first_name
        lastname = update.effective_user.last_name
        language = update.effective_user.language_code

        # Create a new user object
        user_object = Client(chat_id=chat_id,
                             firstname=firstname,
                             lastname=lastname,
                             lang=language,
                             status=Workflows.Idle.IDLE_0)
        
        # Get the current timestamp
        timestamp = datetime.now()

        # Create a new Telegram user dictionary
        new_tguser = {
            'chat_id': chat_id,
            'user_object': user_object,
            'create_time': timestamp,
            'last_accessed': timestamp
        }

        # Add the new user to the active_chats list
        active_chats.append(new_tguser)
        logger.info(f"New chat added: {new_tguser}")
        
        return new_tguser

    except Exception as e:
        logger.error(f"Error adding chat for user {update.effective_user.id}: {e}")
        raise


def get_client_object(update: Update, chat_id) -> Client:
    """
    Retrieves the client object associated with the given chat_id.

    Args:
        update (Update): The update object representing an incoming update.
        chat_id (int): The chat ID to look up in active_chats.

    Returns:
        Client: The client object if found, None otherwise.
    """
    try:
        for client in active_chats:
            if client['chat_id'] == chat_id:
                client_object = client['user_object']
                logger.info(f"Client object found for chat_id {chat_id}")
                return client_object
        
        logger.warning(f"No client object found for chat_id {chat_id}")
        return None
    
    except Exception as e:
        logger.error(f"Error retrieving client object for chat_id {chat_id}: {e}")
        raise


def check_chat(update, chat_id):
    """
    Checks if a chat exists in active_chats based on chat_id. Updates last_accessed timestamp if found,
    otherwise adds a new chat using add_chat function.

    Args:
        update (Update): The update object representing an incoming update.
        chat_id (int): The chat ID to check or add.

    Returns:
        dict: The updated or newly added chat dictionary.
    """
    try:
        for tguser in active_chats:
            if tguser['chat_id'] == chat_id:
                timestamp = datetime.now()
                tguser['last_accessed'] = timestamp
                logger.info(f"Chat updated for chat_id {chat_id}")
                return tguser
        
        logger.warning(f"Chat not found for chat_id {chat_id}. Adding new chat.")
        return add_chat(update, chat_id)
    
    except Exception as e:
        logger.error(f"Error checking chat for chat_id {chat_id}: {e}")
        raise


async def send_message(message_obj, context: CallbackContext, message: str) -> None:
    """
    Sends a text message using the provided message_obj. This was implemented to handle
    both, update.message and update.callback_query.message.

    Args:
        message_obj (object): The message object to send the message with.
        context (CallbackContext): The context object for handling callbacks.
        message (str): The text message to send.

    Returns:
        None
    """
    try:
        await message_obj.reply_text(message, parse_mode='HTML')
        logger.info(f"Message sent successfully: {message}")

    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise


async def show_deposit_address(update: Update, context: CallbackContext, chat_id):
    """
    Sends one of the available deposit addresses to the client so the client can make the deposit.

    Args:
        update (Update): The incoming update from Telegram.
        context (CallbackContext): The context object for handling callbacks.
        chat_id (int): The ID of the chat associated with the client.

    Returns:
        None

    Raises:
        ValueError: If neither update.message nor update.callback_query is found.
        Exception: Any other unexpected error during address retrieval or messaging.

    """
    try:
        client: Client = get_client_object(update, chat_id)

        # Determine if the message is from an ordinary chat or a callback query
        if update.message:
            message_obj = update.message
        elif update.callback_query:
            message_obj = update.callback_query.message
        else:
            raise ValueError("Neither update.message nor update.callback_query found.")

        if client.active_deposit_address:
            message = f"⚠️ <b>You already requested a deposit.</b>\n\n"
            message += f"Please send your deposit to the following address:\n"
            message += f"<code>{client.active_deposit_address}</code>\n\n"
            message += f"<b>Please make sure that you send the USDT on the POLYGON (MATIC) Network.</b>\n\n"
            message += f"You will be automatically notified once the deposit has been credited to our account."
            await send_message(message_obj, context, message)
            return

        text = "Preparing deposit address..."
        await message_obj.reply_text(text)

        response_data = database.get_depositaddresses()
        print(f"RESPONSE_DATA: {response_data}")

        deposit_addresses = response_data.get('result', [])

        in_use = False
        for deposit_address in deposit_addresses:
            for chat in active_chats:
                client_object: Client = chat['user_object']
                if client_object.active_deposit_address and client_object.active_deposit_address == deposit_address['address']:
                    in_use = True
                    break

            if not in_use:
                client.active_deposit_address = deposit_address['address']
                break

        bot_text = "<b><u>Make a Deposit:</u></b>\n\n"
        bot_text += "💳  Please make your deposit to this address:\n\n" + f"<code>{client.active_deposit_address}</code>\n"
        bot_text += "\n"
        bot_text += "<b>Please make sure that you send the USDT on the POLYGON (MATIC) Network.</b>"
        bot_text += "\n\n"
        bot_text += "You will be automatically notified once the deposit has been credited to our account."

        await message_obj.reply_text(bot_text, parse_mode='HTML')

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logger.error(error_message)
        # Notify admin or handle the error appropriately
        await send_message(message_obj, context, error_message)


async def get_factor():
    # fetches current factor from Result server app via get_factor integration endpoint
    # Get the current date
    now = datetime.now()
    # Format the date as "YYYY-MM-DD"
    return_date = now.strftime("%Y-%m-%d")
    # get the current minute
    minute = now.hour * 60 + now.minute

    factor = 0
    url = f"{CONFIG.RETURNS_API.APPSERVER_URL}{CONFIG.RETURNS_API.GET_FACTOR}"
    params = {
        "return_date": return_date,
        "minute": minute
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()  # Parse the response as JSON
        
        if data["status"] == "success":
            factor = data["factor"]
        else:
            print("Error:", data["message"])
            factor = 0
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

    return factor, now


async def get_balance(chat_id: int):
    """
    Fetches the balance, firstname, lastname and currency through
    remote integration endpoint from the Returns server app
    """
    balance_url = f"{CONFIG.RETURNS_API.APPSERVER_URL}{CONFIG.RETURNS_API.GET_BALANCE}"
    balance_data = {
        "chat_id": chat_id  # Ensure chat_id is the correct parameter name
    }

    try:
        response = requests.post(balance_url, json=balance_data)
        response.raise_for_status()  # Raise an error for bad status codes
        balance_data = response.json()  # Parse the response as JSON
        balance_info = balance_data['balance'][0]

        if balance_data["status"] == "success":
            firstname = balance_info['firstname']
            lastname = balance_info['lastname']
            currency = balance_info['currency']
            if balance_info['balance'] is not None:
                balance = Decimal(balance_info['balance'])  # Convert balance to Decimal
            else:
                balance = 0
            last_update_date = balance_info['last_update_date']
        else:
            print("Error:", balance_data["message"])
            balance_info = {}
        
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as e:                

        # if callback == False:
        #     await update.message.reply_text("The server is currently offline. Please try later again.")
        # elif callback == True:
        #     await update.callback_query.message.reply_text("The server is currently offline. Please try later again.")
        # context.user_data['status'] = None

        message = f"Server is offline. Please try again later.\n {e}"
        bot_message = "<b> ⚠️     Maintenance Information     ⚠️ \n\nThis part of the application is currently offline.\nPlease try later again.\n\nWe apologize for the inconvenience.</b>"
        logger.error(message)
        await application.bot.send_message(chat_id=chat_id, text=bot_message, parse_mode='HTML')
        balance = -1
        firstname, lastname, currency = "", "", ""

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        balance_info = {}
        balance = -1
        firstname, lastname, currency = "", "", ""
    
    return balance, firstname, lastname, currency


async def get_factorized_balance(update: Update, context: CallbackContext):
    """
    Retrieves and returns the factorized1 balance information for a client from the database.

    Args:
        update (Update): The incoming update from Telegram containing the message.
        context (CallbackContext): The context object for handling callbacks.

    Returns:
        factorized_balance

    Raises:
        Exception: If there is an error retrieving or formatting the balance information.

    """
    callback = False
    if update.message:
        chat_id = update.message.chat_id
        callback = False
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
        callback = True

    try:
        # fetches oscillation factor
        factor, now = await get_factor()

        # fetches latest closure balance
        balance, firstname, lastname, currency = await get_balance(chat_id)
        if balance == -1:
            user_data = context.user_data
            context.user_data['status'] = None
            await update.message.reply_text("An error occurred while fetching the balance information.")
            return

        factor_decimal = Decimal(factor)

        # Calculate the factorized balance
        factorized_balance = balance * factor_decimal

        factorized_balance = factorized_balance.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
        return factorized_balance
    
    except Exception as e:
        error_message = f"Error retrieving balance information: {str(e)}"
        logger.error(error_message)
        await update.message.reply_text("An error occurred while fetching the balance information.")
        return -1


async def show_balance(update: Update, context: CallbackContext):
    """
    Retrieves and displays the factorized balance information for a client from the database.

    Args:
        update (Update): The incoming update from Telegram containing the message.
        context (CallbackContext): The context object for handling callbacks.

    Returns:
        None

    Raises:
        Exception: If there is an error retrieving or formatting the balance information.

    """
    callback = False
    if update.message:
        chat_id = update.message.chat_id
        callback = False
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
        callback = True

    try:
        # fetches oscillation factor
        factor, now = await get_factor()

        # fetches latest closure balance
        balance, firstname, lastname, currency = await get_balance(chat_id)
        if balance == -1:
            user_data = context.user_data
            context.user_data['status'] = None
            await update.message.reply_text("An error occurred while fetching the balance information.")
            return
        
        balance = Decimal(balance)
        minimum_deposit = Decimal(CONFIG.DEPOSIT_MINIMUM)

        total_client_deposits = database.get_total_deposits_client(p_chat_id=chat_id)
        deposit_difference = (minimum_deposit - balance).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)

        if total_client_deposits >= minimum_deposit:
            factor_decimal = Decimal(factor)

            # Calculate the factorized balance
            factorized_balance = balance * factor_decimal

            factorized_balance = factorized_balance.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
            timestamp = now.strftime("%Y-%m-%d:%H:%M:%S")
            bot_text = f"ℹ️ Your balance per {timestamp}\n\n<code>Client:  {firstname} {lastname}\nBalance: {currency} {factorized_balance}\n</code>"
            logger.info(f"BOT-TEXT: {bot_text}")
        else:
            balance = balance.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
            timestamp = now.strftime("%Y-%m-%d:%H:%M:%S")
            bot_text = f"ℹ️ Your balance per {timestamp}\n\n<code>Client:  {firstname} {lastname}\nBalance: {currency} {balance}</code>\n\n❗ WARNING: You have deposited less than the required mininmum of USDT {minimum_deposit} and therefore <b><u>your funds are excluded from trading</u></b>.\n\nPlease make another deposit of USDT {deposit_difference} or more using the /deposit command."
            logger.info(f"BOT-TEXT: {bot_text}")

        if not callback:
            #await update.message.reply_text(bot_text, parse_mode='HTML')
            ## UPDATE ALL BOT COMMUNICATIONS LIKE THE LINE BELOW FOR NON-CALLBACK MESSAGES, to ensure it uses rate limit queuing
            await context.bot.send_message(chat_id=update.effective_chat.id, text=bot_text, parse_mode='HTML')
        elif callback:
            #await update.callback_query.message.reply_text(bot_text, parse_mode='HTML')
            ## UPDATE ALL BOT COMMUNICATIONS LIKE THE LINE BELOW FOR CALLBACK MESSAGES, to ensure it uses rate limit queuing
            await context.bot.send_message(chat_id=update.callback_query.message.chat_id, text=bot_text, parse_mode='HTML')
    except Exception as e:
        error_message = f"Error retrieving balance information: {str(e)}"
        logger.error(error_message)
        await update.message.reply_text("An error occurred while fetching the balance information.")


async def request_withdrawal(update: Update, context: CallbackContext):
    """
    Initiates a request for withdrawal process for the client.

    Args:
        update (Update): The incoming update from Telegram containing the message.
        context (CallbackContext): The context object for handling callbacks.

    Returns:
        None

    """

    callback = False
    if update.message:
        chat_id = update.message.chat_id
        callback = False
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
        callback = True
    
    try:
        # Make the GET request to the endpoint with chat_id as a parameter
        response = requests.get(CONFIG.ENDPOINT_BASEURL + f'/busy_withdrawal?chat_id={chat_id}')

        # Check if the request was successful (status code 200) and parse the JSON response
        if response.status_code == 200:
            data = response.json()
            wrid = data.get('wrid')  # Assuming the response JSON has a key 'wrid'

            if wrid is not None:
                bot_text = (
                    f"⚠️ You already have a withdrawal request with ID#{wrid} in 'pending' state. "
                    f"Therefore, another withdrawal request cannot be created at this time."
                )
                context.user_data['status'] = None
                if callback == False:
                    await update.message.reply_text(bot_text, parse_mode='HTML')
                elif callback == True:
                    await update.callback_query.message.reply_text(bot_text, parse_mode='HTML')
            else:
                context.user_data['status'] = 'withdrawal: awaiting amount'
                bot_text = f"Enter withdrawal amount or 'cancel' to exit the process:"
                if callback == False: 
                    await update.message.reply_text(bot_text, parse_mode='HTML')
                elif callback == True:
                    await update.callback_query.message.reply_text(bot_text, parse_mode='HTML')
        else:
            # Handle the case where the request failed
            context.user_data['status'] = None
            bot_text = "⚠️ Unable to fetch withdrawal status at this time. Please try again later."
            if callback == False:
                await update.message.reply_text(bot_text, parse_mode='HTML')
            elif callback == True:
                await update.callback_query.message.reply_text(bot_text, parse_mode='HTML')
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as e:                
        error_message = f"Error processing withdrawal request: {str(e)}"
        logger.error(error_message)
        if callback == False:
            await update.message.reply_text("The server is currently offline. Please try later again.")
        elif callback == True:
            await update.callback_query.message.reply_text("The server is currently offline. Please try later again.")
        context.user_data['status'] = None
        return
        
    except Exception as e:
        error_message = f"Error processing withdrawal request: {str(e)}"
        logger.error(error_message)
        if callback == False:
            await update.message.reply_text("An error occurred while processing your withdrawal request.")
        elif callback == True:
            await update.callback_query.message.reply_text("An error occurred while processing your withdrawal request.")


async def client_commit_to_deposit(chat_id, context: CallbackContext):
    """
    Sends a message to the client asking for commitment to proceed with the deposit.

    Args:
        chat_id (int): The ID of the chat with the client.
        context (CallbackContext): The context object for handling callbacks.

    Returns:
        None

    Raises:
        Exception: If there is an error sending the message.

    """
    try:
        keyboard_options = [
            [
                InlineKeyboardButton(
                    "confirm",
                    callback_data=json.dumps({
                        "status": "client_commit_to_deposit",
                        "decision": "yes",
                    })
                ),
                InlineKeyboardButton(
                    "I am not ready yet",
                    callback_data=json.dumps({
                        "status": "client_commit_to_deposit",
                        "decision": "no",
                    })
                )
            ]
        ]
        unit = "seconds" if CONFIG.DEPOSIT_ADDR_VALIDITY / 60 < 1 else "minute" if CONFIG.DEPOSIT_ADDR_VALIDITY / 60 == 1 else "minutes"
        value = CONFIG.DEPOSIT_ADDR_VALIDITY if CONFIG.DEPOSIT_ADDR_VALIDITY < 60 else int(CONFIG.DEPOSIT_ADDR_VALIDITY/60)
        text = f"❓ Make a Deposit\n\nDue to high demand, the deposit address necessary to make a deposit will only be reserved for {value} {unit}.\n\n<b>IMPORTANT:\nPlease make sure that you send the USDT on the POLYGON (MATIC) Network.</b>\n\nPlease confirm if you are ready to make the deposit now."
        reply_markup = InlineKeyboardMarkup(keyboard_options)
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')

    except Exception as e:
        error_message = f"Error occurred while sending deposit confirmation message: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)


async def client_confirm_withdrawal(chat_id, context: CallbackContext):
    """
    Sends a message to the client confirming the withdrawal details and asking for confirmation.

    Args:
        chat_id (int): The ID of the chat with the client.
        context (CallbackContext): The context object for handling callbacks.

    Returns:
        None

    Raises:
        Exception: If there is an error sending the message or retrieving withdrawal data.

    """
    try:
        # Define keyboard options with 'confirm' and 'cancel' buttons
        keyboard_options = [
            [
                InlineKeyboardButton(
                    "confirm",
                    callback_data=json.dumps({
                        "status": "withdrawal: confirm",
                        "decision": "yes",
                    })
                ),
                InlineKeyboardButton(
                    "cancel",
                    callback_data=json.dumps({
                        "status": "withdrawal: confirm",
                        "decision": "no",
                    })
                )
            ]
        ]
        
        # Retrieve withdrawal data for the client
        withdrawal = withdrawals.get_withdrawal_data(chat_id)
        logger.info(f"RAW WITHDRAWAL DATA: {withdrawal}")
        
        # Construct message with withdrawal details
        text = f"❓ Withdraw funds\n\nRequested amount: USDT {withdrawal['amount']}\nBeneficiary wallet: {withdrawal['wallet']}\n\n Please confirm with 'confirm' to proceed or 'cancel' to cancel the withdrawal."
        
        # Send message with inline keyboard for confirmation
        reply_markup = InlineKeyboardMarkup(keyboard_options)
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')
    
    except Exception as e:
        # Handle any exceptions that occur during message sending or data retrieval
        error_message = f"Error occurred in client_confirm_withdrawal: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)

async def fetch_client_from_api(chat_id):
    try:
        # Prepare the payload for the POST request
        payload = {"chat_id": chat_id}
        
        # Make the API call to get client details
        response = requests.post(f"{CONFIG.RETURNS_API.APPSERVER_URL}{CONFIG.RETURNS_API.GET_CLIENT}", json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the response JSON
        client = response.json()
        
        return client
    except requests.HTTPError as e:
        logging.error(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"Error occurred while requesting client details: {e}")
        return None


async def admin_confirm_payout(chat_id, chat_id_client, amount, context: CallbackContext):
    """
    Sends a message to the admin confirming the payout details and asking for confirmation.

    Args:
        chat_id (int): The ID of the admin chat.
        chat_id_client (int): The ID of the client chat.
        amount (float): The amount of USDT to be paid out.
        context (CallbackContext): The context object for handling callbacks.

    Returns:
        None

    Raises:
        Exception: If there is an error sending the message or retrieving client data.

    """
    try:
        # Define keyboard options with 'confirm' and 'cancel' buttons
        keyboard_options = [
            [
                InlineKeyboardButton(
                    "confirm",
                    callback_data=json.dumps({
                        "status": f"payout: confirm{chat_id_client}",
                        "decision": "yes",
                    })
                ),
                InlineKeyboardButton(
                    "cancel",
                    callback_data=json.dumps({
                        "status": f"payout: confirm{chat_id_client}",
                        "decision": "no",
                    })
                )
            ]
        ]
        
        # Retrieve client data for the payout
        client = await fetch_client_from_api(chat_id_client)
        print(f"\n\n\nCLIENT = {client}\n\n\n")
        logger.info(f"Retrieved client data for payout confirmation: {client}")
        
        # Construct message with payout details
        text = f"❓ Payout funds\n\nClient: {client['firstname']} {client['lastname']}\n\nAmount: USDT {amount}\n Please confirm with 'confirm' to proceed or 'cancel' to cancel the payout."
        
        # Send message with inline keyboard for confirmation
        reply_markup = InlineKeyboardMarkup(keyboard_options)
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')
    
    except Exception as e:
        # Handle any exceptions that occur during message sending or client data retrieval
        error_message = f"Error occurred in admin_confirm_payout: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)


async def send_group_chat_invite_link(update: Update, context: CallbackContext) -> None:
    """
    Sends an invitation link to the user for joining the AlgoEagle group chat.
    """
    invite_link = CONFIG.CHAT_GROUP
    message = (
        "Click on the link below to join the AlgoEagle group chat:\n"
        f"{invite_link}"
    )
    await update.callback_query.message.reply_text(message)


async def send_faq(update: Update, context: CallbackContext) -> None:
    """
    Sends the FAQW text.
    """
    faq_text = CONFIG.TEXTS.FAQ
    message = (
        faq_text
    )
    await update.callback_query.message.reply_text(message, parse_mode='HTML')


async def start_chat_with_support(update: Update, context: CallbackContext) -> None:
    """
    Sends an invitation link for joining the support group chat
    """
    invite_link = CONFIG.SUPPORT_CONTACT
    message = (
        "Click on the link below to start a chat with the AlgoEagle support:\n"
        f"{invite_link}"
    )
    await update.callback_query.message.reply_text(message)


async def get_statistics(update: Update, context: CallbackContext):
    print("GET_STATISTICS")
    model = DataHandler()
    chat_id = update.callback_query.message.chat_id
    print(f"CHAT_ID: {chat_id}")
    r_day = model.get_bot_returns_yesterday()
    r_week = model.calculate_weekly_compounded_return()
    r_month = model.calculate_monthly_compounded_return()
    r_threemonths = model.calculate_three_months_compounded_return()

    message = (
        "<b>⭐  ⭐ ⭐   ALGOEAGLE BOT PROFIT   ⭐ ⭐  ⭐\n\n</b>"
        f"These are the returns the AlgoEagle bot has earned.\n\n"
        f"<b>Yesterday:</b>\n"
        f"<code>Date:        {r_day['profit_date']}\n"
        f"Profit:      {r_day['yesterdays_return']}%\n\n</code>"
        f"<b>Last week:</b>\n"
        f"<code>Start date:  {r_week['start_date']}\n"
        f"End date:    {r_week['end_date']}\n"
        f"Profit:      {r_week['compounded_return']}%\n\n</code>"
        f"<b>Last month:</b>\n"
        f"<code>Start date:  {r_month['start_date']}\n"
        f"End date:    {r_month['end_date']}\n"
        f"Profit:      {r_month['compounded_return']}%\n\n</code>"
        f"<b>Last three months:</b>\n"
        f"<code>Start date:  {r_threemonths['start_date']}\n"
        f"End date:    {r_threemonths['end_date']}\n"
        f"Profit:      {r_threemonths['compounded_return']}%\n\n</code>"
    )
    await update.callback_query.message.reply_text(message, parse_mode='HTML')


async def execute_workflow_action(update: Update, context: CallbackContext, action: str) -> None:
    """
    Executes workflow actions based on the provided action string.

    Args:
        update (Update): The update object from Telegram.
        context (CallbackContext): The context object for handling callbacks.
        action (str): The action string indicating which workflow action to execute.

    Returns:
        None

    Raises:
        Exception: If there is an error during execution of any workflow action.

    """
    try:
        if update.message:
            chat_id = update.message.chat_id
        elif update.callback_query:
            chat_id = update.callback_query.message.chat_id
        
        # Ensure the chat exists or add it if it doesn't
        check_chat(update, chat_id)
        
        # Retrieve client object for the chat
        client = get_client_object(update, chat_id)
        
        # Log client data and action string
        logger.info(f"Client DATA: {client.firstname} {client.lastname}, Balance: {client.balance}, Status: {client.status}, Chat ID: {client.chat_id}")
        logger.info(f"ACTION STRING: {action}")
        
        # Execute actions based on the action string
        if action == 'start':
            client.status = Workflows.Start.MENU_0
            logger.info(f"CLIENT STATUS: {client.status}")
            await start(update, context)
        
        elif action == 'show_deposit_address':
            client.status = Workflows.RequestDeposit.SDA_0
            logger.info(f"CLIENT STATUS: {client.status}")
            await client_commit_to_deposit(chat_id, context)
            logger.info(f"Executed show_deposit_address action")
        
        elif action == 'show_balance':
            client.status = Workflows.GetBalance.GEB_0
            await show_balance(update, context)
        
        elif action == 'request_withdrawal':
            client.status = Workflows.Withdraw.WDR_0
            await request_withdrawal(update, context)
        
        elif action == 'show_command_list':
            client.status = Workflows.Idle.IDLE_0
            logger.info(f"CLIENT STATUS: {client.status}")
            await help(update, context)
        elif action == Workflows.GotoChat.GOC_0['function']:
            await send_group_chat_invite_link(update, context)
        elif action == Workflows.GotoFAQ.GOF_0['function']:
            await send_faq(update, context)
        elif action == Workflows.ContactSupport.COS_0['function']:
            await start_chat_with_support(update, context)
        elif action == Workflows.GetStatistics.GES_0['function']:
            await get_statistics(update, context)
        
        else:
            await update.message.reply_text("Invalid action requested.")
    
    except Exception as e:
        # Handle any exceptions that occur during execution
        error_message = f"Error occurred in execute_workflow_action: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)


async def button(update: Update, context: CallbackContext) -> None:
    """
    Handles callback queries from inline buttons.

    Args:
        update (Update): The update object from Telegram containing the callback query.
        context (CallbackContext): The context object for handling callbacks.

    Returns:
        None

    Raises:
        Exception: If there is an error during handling of the callback query.

    """
    try:
        if update.message:
            chat_id = update.message.chat_id
        elif update.callback_query:
            chat_id = update.callback_query.message.chat_id
        
        query = update.callback_query
        
        # Log chat_id for callback update
        logger.info(f"BUTTON CALLBACK UPDATE CHAT_ID: {query.message.chat_id}")
        
        callback_data_json = query.data
        print(f"\n\n\n\ncallback_data_json:\n\n{callback_data_json}\n\n\n\n")
        
        if callback_data_json:
            # Deserialize callback data
            callback_data = json.loads(callback_data_json)
            status = callback_data.get("status")
            decision = callback_data.get("decision")

            # Acknowledge the callback query
            await query.answer()


            if status == "client_commit_to_deposit":
                if decision == "yes":
                    await query.edit_message_text(text="You confirmed to make a deposit now.")
                    await query.message.reply_text("Please wait while your deposit address is being prepared...")
                    client = get_client_object(update, chat_id)
                    deposit_request = await depositstack.add_deposit_request(update, client)
                    
                    # Simulate payment (for testing purposes)
                    # await testunit.make_payment(asset=CONFIG.ASSET, 
                    #                             method=CONFIG.METHOD,
                    #                             eta=deposit_request['eta'],
                    #                             deposit_address=deposit_request['deposit_address'])
                else:
                    await query.edit_message_text(text="You decided to not make a deposit yet.")
                    await query.message.reply_text("You're welcome any time to request to make a deposit.")
            
            elif status == "request_deposit":
                action = "show_deposit_address"
                await execute_workflow_action(update, context, action)
            elif status == "get_balance":
                action = "show_balance"
                await execute_workflow_action(update, context, action)
            elif status == "request_withdraw":
                action = "request_withdrawal"
                await execute_workflow_action(update, context, action)
            elif status == Workflows.GotoChat.GOC_0['function']:
                action = Workflows.GotoChat.GOC_0['function']
                await execute_workflow_action(update, context, action)
            elif status == Workflows.GetStatistics.GES_0['function']:
                action = Workflows.GetStatistics.GES_0['function']
                await execute_workflow_action(update, context, action)
            elif status == Workflows.GotoFAQ.GOF_0['function']:
                action = Workflows.GotoFAQ.GOF_0['function']
                await execute_workflow_action(update, context, action)
            elif status == Workflows.ContactSupport.COS_0['function']:
                action = Workflows.ContactSupport.COS_0['function']
                await execute_workflow_action(update, context, action)            
            elif status == "withdrawal: confirm":
                if decision == "yes":
                    client_raw = await fetch_client_from_api(chat_id)
                    client = client_raw["client"][0]
                    print(f"\n\n\nCLIENT = {client}\n\n\n")
                    withdrawal = withdrawals.get_withdrawal_data(chat_id)
                    amount = withdrawal['amount']
                    wallet = withdrawal['wallet']
                    balance, _, _, _ = await get_balance(chat_id)
                    formatted_balance = f"{balance:.6f}"  # Format balance to 6 decimal places
                    
                    # Prepare data to send to sister application
                    data = {
                        "chat_id": chat_id,
                        "firstname": client['firstname'],
                        "lastname": client['lastname'],
                        "currency": "USDT",
                        "amount": amount,
                        "wallet": wallet
                    }

                    await query.edit_message_text(text=f"You confirmed to request a withdrawal:\n\nChat-ID: {chat_id}\nFirstname: {client['firstname']}\nLastname: {client['lastname']}\nCurrency: USDT\nAmount: {amount}\nWallet: {wallet}")

                    # Send post request to sister application
                    url = CONFIG.ENDPOINT_BASEURL + "/request_withdrawal"
                    headers = {'Content-Type': 'application/json'}
                    response = requests.post(url, headers=headers, data=json.dumps(data))
                    logger.info(f"RESPONSE FROM INTEGRATION ENDPOINT: {response}")
                    
                    context.user_data['status'] = None  # Reset status because user process ends here
                    
                    message = (
                        f"<b>🔴 WITHDRAWAL REQUEST 💵</b>\n\n"
                        f"By user: {client['firstname']} {client['lastname']}\n"
                        f"Telegram user-id: <code>{chat_id}</code>\n"
                        f"Balance USDT {formatted_balance}\n"
                        f"Withdrawal amount: USDT <code>{amount}</code>\n"
                        f"Beneficiary account: <code>{wallet}</code>"
                    )
                    for admin_chat_id in CONFIG.ADMIN_CHAT_IDS:
                        try:
                            await depositstack.bot_message(chat_id=admin_chat_id, message=message)
                        except Exception as e:
                            error_message =f"Error occured sending admin notifications: {str(e), admin_chat_id}"
                            logger.error(error_message)
                    
                    message = f"Your request to withdraw USDT {str(amount)} was forwarded to the administrator."
                    await depositstack.bot_message(chat_id=chat_id, message=message)
                    
                    withdrawals.remove_withdrawal(chat_id)
                
                else:
                    await query.edit_message_text(text=f"You clicked on cancel.")
                    context.user_data['status'] = None  # Reset status because user process ends here
                    withdrawals.remove_withdrawal(chat_id)
                    message = "Withdrawal canceled by user."
                    await depositstack.bot_message(chat_id=chat_id, message=message)
    
    except Exception as e:
        # Handle any exceptions that occur during execution
        error_message = f"Error occurred in button callback handling: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)


async def poll_deposit_request_stack():
    """
    Coroutine function to continuously process deposit requests from a stack.

    This function runs indefinitely, processing each deposit request from a stack
    at a defined interval (CONFIG.DEPOSIT_REQUEST_STACK_INTERVAL).

    Raises:
        Exception: If there is an unexpected error during processing of deposit requests.
    """
    while not application.shutdown_event.is_set():
        print(f"\n\n\n\nSHUTDOWN FLAG: {application.shutdown_event.is_set}\n\n\n\n")
        try:
            # Process the next deposit request from the stack
            next_request = await depositstack.process_next()
            
            # Sleep for the configured interval before processing the next request
            await asyncio.sleep(CONFIG.DEPOSIT_REQUEST_STACK_INTERVAL)
            if application.shutdown_event.is_set():
                break


        except Exception as e:
            # Handle any exceptions that occur during processing
            error_message = f"Error occurred in deposit request stack polling: {str(e)}"
            logger.error(error_message)
            raise Exception(error_message)


async def process_transfers(deposits):
    if deposits:
        usdt = Funds.USDT() 
        for deposit in deposits: # loop that transfers the deposits on the deposit-accounts to the central collection account
            logger.info(f"TRANSFERRING DEPOSIT USDT {deposit['amount']} FROM_ADDRESS: {deposit['deposit_address']} TO CENTRAL ACCOUNT")
            usdt.transfer(from_address=deposit['deposit_address'], amount=deposit['amount'], deposit_tx_id=deposit['refid']) 
            await asyncio.sleep(2)
       
        

async def poll_recent_deposits():
    """
    Coroutine function to poll recent deposits from an API or test source periodically.
    
    This function runs indefinitely, polling recent deposits at a defined interval (CONFIG.DEPOSIT_POLLING_INTERVAL).
    It uses an API or test source (KrakenAPI or testunit) to fetch recent deposit data.
    The fetched data is then processed and added to a deposit stack for further handling.

    Raises:
        Exception: If there is an unexpected error during API request, data processing, or deposit handling.
    """
    api = EthAPI()
    while not application.shutdown_event.is_set():
        try:
            # Uncomment the line below for production use:
            # response_data = await api.get_recent_deposits(asset=CONFIG.ASSET, method=CONFIG.METHOD)
            
            # For test use only:
            # response_data = testunit.get_recent_deposits(asset=CONFIG.ASSET, method=CONFIG.METHOD)

            # get balances from all Polygon Mainnet deposit addresses
            all_balances = api.get_recent_deposits()

            # create new list consisting of dict containing address and balance where balance > 0
            # we want to process only those accounts that actually have a balance.
            active_deposits = []
            for balance in all_balances:
                if balance['balance'] > 0:
                    row = {
                        'deposit_address': balance['deposit_address'],
                        'balance': balance['balance']
                        }
                    active_deposits.append(row)
                    print(f"\n\n\nDEPOSIT FOUND {balance['balance']} AT DEPOSIT-ADDRESS: {balance['deposit_address']}")
            
            logger.info(f"New deposits found: {active_deposits}")
            deposit_logs = DepositLogs(active_deposits)
            deposit_logs.fetch_logs() # we get the new logs and insert those into the depositlogs table
            response_data = []

            depositlogs = database.get_newdepositlogs() # we get the new depositlogs with transfer == False from the database
            print(f"\n\n\n\n\n#####################################################")
            if depositlogs:
                print(f"Found new deposits:\n{depositlogs}")
            else:
                print(f"No new deposits found")
            print(f"#####################################################\n\n\n\n\n")

            if depositlogs: # only process if there are any new deposits where transfer == False
                # process the depositlogs and build response_data
                for deposit in depositlogs:
                    row =  {
                        'deposit_address': deposit['to_address'],
                        'asset': 'USDT',
                        'txid': deposit['from_address'],
                        'amount': deposit['amount'],
                        'refid': deposit['transaction_id'],  # Unique identifier of the deposit transaction
                        'credit_time_timestamp': deposit['block_timestamp'],
                        'credit_time': deposit['created_at']  
                    }
                    response_data.append(row)            

                # call async payment processing function
                asyncio.create_task(process_transfers(response_data))

                # Process the received deposit data and add it to the deposit stack
                await depositstack.receive_deposit(response_data)
            
            # Sleep for the configured interval before polling again
            await asyncio.sleep(CONFIG.DEPOSIT_POLLING_INTERVAL)
            if application.shutdown_event.is_set():
                break
        
        except Exception as e:
            error_message = f"Error occurred in poll_recent_deposits coroutine: {str(e)}"
            logger.error(error_message)
            raise Exception(error_message)


async def handle_text_input(update: Update, context: CallbackContext):
    """
    Handles text input from the user during a specific operation, such as withdrawal.

    Args:
        update (Update): The incoming update object containing message information.
        context (CallbackContext): The context object for handling the conversation.

    Raises:
        Exception: If there is an unexpected error during the operation handling.
    """
    try:
        chat_id = update.message.chat_id
        user_data = context.user_data

        # Cancel the ongoing operation if the user types 'cancel'
        if update.message.text == 'cancel':
            if 'status' not in user_data:
                user_data['status'] = None
            if user_data['status'] is not None:
                await update.message.reply_text(f'The current operation "{user_data["status"]}" was canceled.')
                context.user_data['status'] = None
            else:
                await update.message.reply_text("Nothing to cancel: No ongoing operation.")
            return

        # Handle withdrawal amount input if the user is in 'withdrawal: awaiting amount' status
        if user_data['status'] == 'withdrawal: awaiting amount':
            if update.message.text:
                amount_text = update.message.text
                try:
                    amount = float(amount_text)
                    context.user_data['status'] = ''  # Reset status after successfully parsing amount
                    balance_raw = await get_factorized_balance(update, context)
                    if balance_raw == -1: # if get_balance failed, server probably not online, therefore cancel process
                        user_data = context.user_data
                        user_data['status'] = None
                        await update.message.reply_text("Withdrawal process was canceled. Please try later again.")
                        return

                    print(f"\n\n\n\nRESULT:\n{balance_raw}")
                    if balance_raw:
                        balance = float(balance_raw)
                        if amount > balance:
                            await update.message.reply_text(f"The requested withdrawal amount {amount} exceeds your balance of {balance}. Please enter a lower amount or enter 'cancel' to cancel the withdrawal.")
                            context.user_data['status'] = 'withdrawal: awaiting amount'
                        elif amount == 0:
                            await update.message.reply_text(f"The requested withdrawal amount {amount} must not be 0. Please enter a valid amount or enter 'cancel' to cancel the withdrawal.")
                            context.user_data['status'] = 'withdrawal: awaiting amount'
                        elif amount < 0:
                            await update.message.reply_text(f"The requested withdrawal amount {amount} must not be negative. Please enter a valid amount or enter 'cancel' to cancel the withdrawal.")
                            context.user_data['status'] = 'withdrawal: awaiting amount'
                        else:
                            withdrawals.update_amount(chat_id, amount)
                            message = "Now, please enter your wallet address or enter 'cancel' to cancel the withdrawal:"
                            context.user_data['status'] = 'withdrawal: awaiting wallet'
                            await depositstack.bot_message(chat_id=chat_id, message=message)
                except ValueError:
                    await update.message.reply_text("Please enter a valid amount or enter 'cancel' to cancel the withdrawal.")

        # Handle wallet address input if the user is in 'withdrawal: awaiting wallet' status
        elif user_data['status'] == 'withdrawal: awaiting wallet':
            logger.info('AWAITING WALLET!!!!')
            if update.message.text:
                wallet_address = update.message.text
                is_valid_wallet_address = await validate_address(wallet_address, chat_id)
                logger.info(f"is_valid_wallet_address: {is_valid_wallet_address}")
                if not is_valid_wallet_address:
                    message = f"The wallet address '{wallet_address}' you entered is not a valid USDT/ERC20 wallet address.\nPlease enter a correct wallet address or enter 'cancel' to cancel the process entirely."
                    await depositstack.bot_message(chat_id=chat_id, message=message)
                    context.user_data['status'] = 'withdrawal: awaiting wallet'
                    return
                withdrawals.update_wallet(chat_id, wallet_address)
                await client_confirm_withdrawal(chat_id, context)
    
    except Exception as e:
        error_message = f"Error occurred in handling text input: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)



def start_telegram_bot():
    """
    Starts the Telegram bot application with configured command and message handlers.

    This function initializes the Telegram bot application using the provided bot token from CONFIG.TELEGRAM_KEY.
    It adds command handlers for start, deposit, balance, withdraw, and help commands, each triggering specific workflow actions.
    Additionally, it adds a callback query handler and a text message handler for handling user interactions.

    Raises:
        Exception: If there is an unexpected error during the bot initialization or polling.
    """

    try:
        # commented out because used before introduction of telegram_bot.py
        # global application
        # application = Application.builder().token(CONFIG.TELEGRAM_KEY).build()

        # Add command handlers
        application.add_handler(CommandHandler("start", lambda update, context: execute_workflow_action(update, context, Workflows.Start.MENU_0['function'])))
        application.add_handler(CommandHandler("deposit", lambda update, context: execute_workflow_action(update, context, Workflows.RequestDeposit.SDA_0['function'])))
        application.add_handler(CommandHandler("balance", lambda update, context: execute_workflow_action(update, context, Workflows.GetBalance.GEB_0['function'])))
        application.add_handler(CommandHandler("withdraw", lambda update, context: execute_workflow_action(update, context, Workflows.Withdraw.WDR_0['function'])))
        application.add_handler(CommandHandler("help", lambda update, context: execute_workflow_action(update, context, Workflows.GetHelp.HLP_0['function'])))

        # Add callback query handler
        application.add_handler(CallbackQueryHandler(button))

        # Add text message handler
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

        # Start the Telegram bot polling
        application.run_polling()

    except Exception as e:
        error_message = f"Error occurred while starting Telegram bot: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)


def main():
    """
    Main entry point to start the application.

    This function starts multiple threads for different components of the application:
    - FastAPI server using a separate thread.
    - Polling recent deposits using another thread.
    - Polling deposit request stack using a third thread.
    Finally, it starts the Telegram bot in the main thread and waits for the deposit polling thread to finish.

    Raises:
        Exception: If there is an unexpected error during thread creation or bot startup.
    """


    global thread_fastapi, thread_deposits, thread_deposit_request_stack

    try:
        with executor:
            # submit tasks to run in background as separate threads
            executor.submit(run_fastapi)
            executor.submit(poll_recent_deposits_wrapper)
            executor.submit(poll_deposit_request_stack_wrapper)

            # Start the Telegram bot in the main thread
            start_telegram_bot()

    except Exception as e:
        error_message = f"Error occurred in main function: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)


def poll_recent_deposits_wrapper():
    """
    Wrapper function to run the poll_recent_deposits coroutine in a dedicated event loop.

    This function creates a new asyncio event loop, sets it as the current loop,
    and runs the poll_recent_deposits coroutine within this loop. It ensures proper
    setup and cleanup of the event loop environment.

    Raises:
        Exception: If there is an unexpected error during event loop creation or execution.
    """
    # Create an asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run the poll_recent_deposits coroutine in the event loop
        loop.run_until_complete(poll_recent_deposits())
    
    finally:
        # Ensure the event loop is closed after execution
        loop.close()


def poll_deposit_request_stack_wrapper():
    """
    Wrapper function to run the poll_deposit_request_stack coroutine in a dedicated event loop.

    This function creates a new asyncio event loop, sets it as the current loop,
    and runs the poll_deposit_request_stack coroutine within this loop. It ensures proper
    setup and cleanup of the event loop environment.

    Raises:
        Exception: If there is an unexpected error during event loop creation or execution.
    """
    # Create an asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run the poll_deposit_request_stack coroutine in the event loop
        loop.run_until_complete(poll_deposit_request_stack())
    
    finally:
        # Ensure the event loop is closed after execution
        loop.close()


def run_fastapi():
    """
    Function to run the FastAPI application using uvicorn.

    This function starts the FastAPI application on the specified host and port.

    Raises:
        Exception: If there is an unexpected error during FastAPI application startup.
    """

    try:
        config = uvicorn.Config("main:app", host="127.0.0.1", port=8000, workers=4)
        server = uvicorn.Server(config)
        server.run()


    except KeyboardInterrupt:
        server.should_exit = True
        server.shutdown()
    except Exception as e:
        error_message = f"Error occurred in run_fastapi function: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)


def shutdown():
    logger.info("Shutting down...")
    application.shutdown_event.set() # sets signal for threads to stop
    logger.info("Stop event set.")
    executor.shutdown(wait=True)
    logger.info("Shutdown complete.")


if __name__ == '__main__':
    # Entry point of the script when executed directly
    # Call the main function to start the application
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received shutdown signal, shutting down.")
        shutdown()

    # Note: This block ensures that the main function is executed when the script is run directly.
    # The main function typically starts multiple threads for FastAPI, Telegram bot, and other asynchronous tasks.
    # It serves as the central starting point for the application, orchestrating various components to run concurrently.
    # Ensure that the main function is correctly implemented to handle the application's lifecycle and event loops.
