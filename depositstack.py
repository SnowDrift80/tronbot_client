# depositstack.py
import sys
import datetime
import requests
from decimal import Decimal
import telegram
from telegram import Update, Bot
from telegram.ext import Updater, CommandHandler, MessageHandler, filters
from client import Client
from config import CONFIG
from model import DataHandler
import logging


logger = logging.getLogger(__name__)



class DepositStack():
    """Class to manage a stack of deposit requests.

    This class is designed to handle deposit requests in a first-in, first-out (FIFO) manner.
    If more people than the maximum number or deposit addresses (CONFIG.MAX_DEPOSIT_ADDRESSES)
    concurrently request deposit addresses, this stack ensures that each request is processed 
    sequentially. The stack allows adding new requests and retrieving them in the order they 
    were added.

    Attributes:
        stack (list): A list to store deposit request dictionaries.

    Methods:
        __init__: Initializes an empty stack.
        add_deposit_request: Adds a new deposit request to the stack.
        process_next: Removes and returns the oldest deposit request from the stack.
        get_all_deposits: Returns the entire stack of deposit requests.
    """


    def __init__(self, database: DataHandler):
        """Initialize the DepositStack object.

        This constructor sets up the necessary components for managing deposit requests.
        It initializes the Telegram bot, retrieves deposit addresses from an API, and
        sets up data structures for managing deposit requests.

        Args:
            database (DataHandler): An instance of DataHandler for database operations.

        Raises:
            ValueError: If the configured MAX_DEPOSIT_ADDRESSES is higher than the number
                        of available deposit addresses retrieved from the API.
            Exception: Any unexpected errors during initialization, including API errors.
        """
        self.bot = telegram.Bot(token=CONFIG.TELEGRAM_KEY)
        self.database = database

        try:
            # Retriete all deposit addresses from database
            depositaddresses_recordset = database.get_depositaddresses()
            print(f"\n\n\nDEPOSITADDRESSES_RECORDSET:\n{depositaddresses_recordset}\n\n\n\n")


            # Check if MAX_DEPOSIT_ADDRESSES is valid
            if CONFIG.MAX_DEPOSIT_ADDRESSES > len(depositaddresses_recordset):
                raise ValueError(f"MAX_DEPOSIT_ADDRESSES ({CONFIG.MAX_DEPOSIT_ADDRESSES}) > than effectively available depositaddresses ({len(depositaddresses_recordset)}).\n\nUpdate config.py MAX_DEPOSIT_ADDRESSES accordingly.")

            # Initialize deposit addresses
            self.deposit_addresses = [depositaddresses_recordset[i]['depositaddress'] for i in range(CONFIG.MAX_DEPOSIT_ADDRESSES)]
            print(f"\n\n\nSELF.DEPOSIT_ADDRESSES:\n{self.deposit_addresses}")
            self.stacks = [[] for _ in range(CONFIG.MAX_DEPOSIT_ADDRESSES)]
            self.deposit_ref_ids = set()  # set more efficient than list in 'in' comparisons

        except ValueError as e:
            # Handle ValueError related to MAX_DEPOSIT_ADDRESSES
            print(f"ValueError in __init__: {e}")
            sys.exit(1)

        except Exception as e:
            # Handle any unexpected errors during initialization
            print(f"Unexpected error in __init__: {e}")
            sys.exit(1)



    async def add_deposit_request(self, update: Update, client: Client):
        """Add a new deposit request to one of the stacks.

        This method creates a deposit request dictionary and adds it to the stack
        with the least number of existing requests. It calculates the estimated time
        when the deposit address will be available and sends a notification to the
        client if their request is queued.

        Args:
            update (Update): The Telegram update object.
            client (Client): The client object representing the user making the request.

        Returns:
            dict: The deposit request dictionary containing 'timestamp', 'eta',
                  'client_obj', 'deposit_address', and 'sent_to_client' keys.
        """
        try:
            timestamp = datetime.datetime.now().isoformat()

            # Buffer as timedelta
            validity_buffer = datetime.timedelta(seconds=CONFIG.DEPOSIT_ADDR_VALIDITY_BUFFER)

            # Find the stack with the least number of requests
            min_stack_index = min(range(CONFIG.MAX_DEPOSIT_ADDRESSES), key=lambda i: len(self.stacks[i]))

            # Calculate ETA for the new request
            if len(self.stacks[min_stack_index]) > 0:
                last_eta = self.stacks[min_stack_index][-1]['eta']
                eta_timestamp = datetime.datetime.fromisoformat(last_eta) + datetime.timedelta(seconds=CONFIG.DEPOSIT_ADDR_VALIDITY) + validity_buffer
                eta = eta_timestamp.isoformat()
            else:
                eta_timestamp = datetime.datetime.fromisoformat(timestamp) + datetime.timedelta(seconds=CONFIG.DEPOSIT_ADDR_VALIDITY) + validity_buffer
                eta = eta_timestamp.isoformat()

            # Create deposit request dictionary
            deposit_request = {
                'timestamp': timestamp,
                'eta': eta,
                'client_obj': client,
                'deposit_address': self.deposit_addresses[min_stack_index],
                'sent_to_client': False
            }

            # Add the request to the appropriate stack
            self.stacks[min_stack_index].append(deposit_request)

            # Notify the client if their request is queued
            if len(self.stacks[min_stack_index]) > 1:
                eta_datetime = datetime.datetime.fromisoformat(eta) - validity_buffer
                human_readable_eta = eta_datetime.strftime("%H:%M")

                message = (
                    f"ℹ️ Thank you for requesting to make a deposit.\n\n"
                    f"Due to high demand and limited payment slots, your deposit request has been queued.\n\n"
                    f"Estimated time to receive the deposit address: {human_readable_eta} or earlier.\n"
                    f"You will have {CONFIG.DEPOSIT_ADDR_VALIDITY / 60:.0f} minutes to complete the deposit once you receive the address."
                )
                chat_id = client.chat_id
                logger(f"SENDING MESSAGE:\n{message}")
                await self.send_message_to_client(message=message, chat_id=chat_id, update=update)
                logger(f"CHAT_ID: {chat_id}")

            return deposit_request
        
        except Exception as e:
            # Handle any unexpected errors during deposit request addition
            logger(f"Error in add_deposit_request: {e}")
            raise


    async def send_message_to_client(self, message, chat_id, update: Update):
        """Send a message to the client on Telegram.

        This method sends a formatted HTML message to the specified chat_id using
        the Telegram bot API. It catches any exceptions that occur during the 
        message sending process.

        Args:
            message (str): The HTML-formatted message to send.
            chat_id (str): The ID of the chat where the message should be sent.
            update (Update): The Telegram update object.

        Raises:
            Exception: If an error occurs while sending the message.
        """
        try:
            logger.info("Sending message to client.")
            await update.message.reply_text(message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error while trying to send message to Telegram user: {e}")
            raise


    async def bot_message(self, chat_id, message: str):
        """Send a message to the client via the Telegram bot.

        This method sends an HTML-formatted message to the specified chat_id
        using the Telegram bot API. It catches any exceptions that occur during 
        the message sending process.

        Args:
            chat_id (str): The ID of the chat where the message should be sent.
            message (str): The HTML-formatted message to send.

        Raises:
            Exception: If an error occurs while sending the message.
        """
        try:
            logger.info("Sending bot message to client.")
            bot = Bot(CONFIG.TELEGRAM_KEY)
            await bot.sendMessage(chat_id, message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error while trying to send message to Telegram user: {e}")
            raise


    def get_all_deposit_requests(self):
        """Retrieve all deposit requests across all stacks.

        Returns:
            list: A list of all deposit requests stored in the stacks.
        """
        try:
            logger.info("Retrieving all deposit requests.")
            return self.stacks
        except Exception as e:
            logger.error(f"Error occurred while retrieving deposit requests: {e}")
            raise
    
    
    async def process_next(self):
        """Process the next deposit request from the stack with the most elements."""
        logger.info("Processing next deposit request.")

        try:
            # Loop through the stacks to find the oldest element
            for stack in self.stacks:
                if not stack:
                    continue  # Skip empty stacks
                
                # Retrieve the oldest element
                element = stack[0]
                start_datetime = datetime.datetime.fromisoformat(element['timestamp'])
                eta_datetime = datetime.datetime.fromisoformat(element['eta'])
                current_timestamp = datetime.datetime.now()
                client_obj: Client = element['client_obj']
                chat_id = client_obj.chat_id

                # Check if the deposit request has been sent to the client
                if not element['sent_to_client']:
                    # Check if it's time to notify the client
                    if start_datetime <= current_timestamp:
                        message = (
                            f"<b><u>ℹ️ Make a Deposit:</u></b>\n\n"
                            f"💳  Please make your deposit to this address:\n\n"
                            f"<code>{element['deposit_address']}</code>\n\n"
                            f"You will be automatically notified once the deposit has been credited to our account."
                        )
                        await self.bot_message(chat_id, message)
                        element['sent_to_client'] = True
                else:
                    # Check if the deposit request has timed out
                    if eta_datetime <= current_timestamp:
                        message = (
                            f"⚠️ <b><u>Deposit Timeout:</u></b>\n\n"
                            f"Your window to make a deposit using the address {element['deposit_address']} has timed out.\n\n"
                            f"If you still wish to make a deposit, please request a new deposit address."
                        )
                        await self.bot_message(chat_id, message)
                        stack.pop(0)
            
        except Exception as e:
            logger.error(f"Error occurred while processing next deposit request: {e}")
            raise


    async def receive_deposit(self, response_data):
        """Process recent deposits and match them with the deposit requests in the stacks."""
        logger.info("Processing recent deposits.")
        
        try:
            print(f"\n\n\n\nRESPONSE_DATA:\n{response_data}\n\n\n\n\n")
            deposits = response_data
            

            for deposit in deposits:
                deposit_address = deposit['deposit_address']
                asset = deposit['asset']
                txid = deposit['txid']
                amount = deposit['amount']
                refid = deposit['refid']  # Unique identifier of the deposit transaction
                credit_time = datetime.datetime.now().isoformat()
                
                # Check if the deposit has already been processed
                if self.database.check_if_deposit_processed(refid):
                    continue
                
                # Iterate through each stack to find matching deposit requests
                for stack in self.stacks:
                    for i, request in enumerate(stack):
                        print(f"request['deposit_address']={request['deposit_address']}   vs   deposit_address: {deposit_address}\n******************************************************************************")
                        if request['deposit_address'] == deposit_address and request['sent_to_client']:
                            client_obj: Client = request['client_obj']
                            first_name = client_obj.firstname
                            last_name = client_obj.lastname
                            chat_id = client_obj.chat_id
                            
                            # Log the deposit received information
                            logger.info(f"Deposit received to deposit address {deposit_address} from client {first_name} {last_name}. Amount: {amount}")
                            
                            # Construct message for deposit confirmation
                            message = (
                                f"<b><u>ℹ️ Deposit Receipt Confirmation:</u></b>\n\n"
                                f"Hello {first_name} {last_name},\n"
                                f"🏦 Your deposit with the amount of <b>USDT {amount}</b> was just credited to our account.\n\n"
                                f"Thank you for your trust and welcome on board!.\nYou can check your balance anytime with the /balance command."
                            )
                            
                            # Add deposit record to the database to prevent re-processing
                            self.database.add_deposit_record(refid, chat_id, first_name, last_name, amount, asset, txid, deposit_address)
                            
                            # Update client balances and create ledger entry / old code
                            # self.database.handle_deposit(chat_id, first_name, last_name, CONFIG.ASSET, CONFIG.METHOD, amount, deposit_address, refid, credit_time, txid)


############################ UPDATE CLIENT BALANCES REMOTE PROCEDURE CALL ##################################################
                            # Update client balances and create ledger entry 
                            # Prepare data to send in the API request
                            # Convert amount to float if it's a Decimal
                            if isinstance(amount, Decimal):
                                amount = float(amount)

                            payload = {
                                "chat_id": chat_id,
                                "firstname": first_name,
                                "lastname": last_name,
                                "currency": CONFIG.ASSET,
                                "method": CONFIG.METHOD,
                                "amount": amount,
                                "deposit_address": deposit_address,
                                "kraken_refid": refid,
                                "kraken_time": credit_time,
                                "kraken_txid": txid
                            }

                            try:
                                # Make the API call to handle the deposit
                                response = requests.post(f"{CONFIG.RETURNS_API.APPSERVER_URL}{CONFIG.RETURNS_API.HANDLE_DEPOSIT}", json=payload)
                                response.raise_for_status()  # Raise an exception for HTTP errors
                                result = response.json()
                                logger.info(f"Deposit handled successfully: {result}")
                            except requests.HTTPError as e:
                                logger.error(f"HTTP error occurred: {e}")
                            except Exception as e:
                                logger.error(f"Error occurred while calling handle_deposit API: {e}")

#############################################################################################################################


                            # Notify client about the deposit confirmation
                            #await self.bot_message(chat_id, message)
                            # use the below variant to use the automatic queuing feature
                            await self.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')

                            
                            # Add refid to known refids to avoid processing it again
                            self.deposit_ref_ids.add(refid)
                            
                            # Remove the processed request from the stack
                            stack.pop(i)
                            break  # Exit the loop after processing the deposit request
                            
        except Exception as e:
            logger.error(f"Error occurred while processing recent deposits: {e}")
            raise
    
    
    def __len__(self):
        """Return the total number of deposit requests across all stacks."""
        try:
            # Sum up the lengths of all stacks to get the total number of deposit requests
            total_requests = sum(len(stack) for stack in self.stacks)
            return total_requests
        except Exception as e:
            # Log any errors that occur during the calculation
            logger.error(f"Error occurred while calculating total number of deposit requests: {e}")
            raise


    def __repr__(self):
        """Return a string representation of the DepositStack object."""
        try:
            # Return a string representation that includes the stacks attribute
            return f"DepositStack(stack={self.stacks})"
        except Exception as e:
            # Log any errors that occur during the representation creation
            logger.error(f"Error occurred while creating string representation of DepositStack: {e}")
            raise
