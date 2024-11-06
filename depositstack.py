# depositstack.py
import re
import sys
from datetime import datetime, timedelta
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


            # Check if MAX_DEPOSIT_ADDRESSES is valid
            if CONFIG.MAX_DEPOSIT_ADDRESSES > len(depositaddresses_recordset):
                raise ValueError(f"MAX_DEPOSIT_ADDRESSES ({CONFIG.MAX_DEPOSIT_ADDRESSES}) > than effectively available depositaddresses ({len(depositaddresses_recordset)}).\n\nUpdate config.py MAX_DEPOSIT_ADDRESSES accordingly.")

            # Initialize deposit addresses
            self.deposit_addresses = [depositaddresses_recordset[i]['depositaddress'] for i in range(CONFIG.MAX_DEPOSIT_ADDRESSES)]
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



    async def add_deposit_request(self, update: Update, client: Client, referral: str = None, multiplier: float = None):
        """Add a new deposit request to one of the stacks.

        This method creates a deposit request dictionary and adds it to the stack
        with the least number of existing requests. It calculates the estimated time
        when the deposit address will be available and sends a notification to the
        client if their request is queued.

        Args:
            update (Update): The Telegram update object.
            client (Client): The client object representing the user making the request.
            referral (str): If referral code is given, include into deposit_request object.

        Returns:
            dict: The deposit request dictionary containing 'timestamp', 'eta',
                  'client_obj', 'deposit_address', and 'sent_to_client' keys.
        """
        try:
            timestamp = datetime.now().isoformat()

            # Buffer as timedelta
            validity_buffer = timedelta(seconds=CONFIG.DEPOSIT_ADDR_VALIDITY_BUFFER)
            deposit_addr_validity = timedelta(seconds=CONFIG.DEPOSIT_ADDR_VALIDITY)

            # Find the stack with the least number of requests
            min_stack_index = min(range(CONFIG.MAX_DEPOSIT_ADDRESSES), key=lambda i: len(self.stacks[i]))

            # Calculate ETA for the new request
            if len(self.stacks[min_stack_index]) > 0:
                last_eta = self.stacks[min_stack_index][-1]['eta']
                etd_timestamp = datetime.fromisoformat(last_eta) + timedelta(seconds=1)
                eta_timestamp = datetime.fromisoformat(last_eta) + deposit_addr_validity + validity_buffer
                eta = eta_timestamp.isoformat()
                etd = etd_timestamp.isoformat()
            else:
                etd_timestamp = datetime.fromisoformat(timestamp)
                eta_timestamp = datetime.fromisoformat(timestamp) + deposit_addr_validity + validity_buffer
                eta = eta_timestamp.isoformat()
                etd = etd_timestamp.isoformat()

            # Create deposit request dictionary
            print(f"\n\n\n\n\n~~~~~~~~~~~~~~~~~~~~~~\n\ndeposit_request eta: {eta}\n~~~~~~~~~~~~~~~~~~~~~~\n\n\n\n\n")
            deposit_request = {
                'timestamp': timestamp,
                'etd': etd, # estimated start time (estimated time of departure) - start time of deposit time window
                'eta': eta, # estimated time as of when the deposit time-window will start - end time of deposit time window
                'client_obj': client,
                'deposit_address': self.deposit_addresses[min_stack_index],
                'sent_to_client': False,
                'referral' : referral,
                'multiplier' : multiplier
            }

            # Add the request to the appropriate stack
            self.stacks[min_stack_index].append(deposit_request)

            # Notify the client if their request is queued
            if len(self.stacks[min_stack_index]) > 1:
                eta_datetime = datetime.fromisoformat(eta) - validity_buffer
                human_readable_eta = eta_datetime.strftime("%H:%M")

                message = (
                    f"ℹ️ Thank you for requesting to make a deposit.\n\n"
                    f"Due to high demand and limited payment slots, your deposit request has been queued.\n\n"
                    f"Estimated time to receive the deposit address: {human_readable_eta} or earlier.\n"
                    f"You will have {CONFIG.DEPOSIT_ADDR_VALIDITY / 60:.0f} minutes to complete the deposit once you receive the address."
                )
                chat_id = client.chat_id
                logger.info(f"SENDING MESSAGE:\n{message}")
                await self.send_message_to_client(message=message, chat_id=chat_id, update=update)
                logger.info(f"CHAT_ID: {chat_id}")

            return deposit_request
        
        except Exception as e:
            # Handle any unexpected errors during deposit request addition
            logger.error(f"Error in add_deposit_request: {e}")


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
    
    
    async def process_next(self):
        """Process the next deposit request from the stack with the most elements."""
        try:
            # Loop through the stacks to find the oldest element
            for stack in self.stacks:
                if not stack:
                    continue  # Skip empty stacks
                
                # Retrieve the oldest element
                element = stack[0]
                start_datetime = datetime.fromisoformat(element['etd'])
                eta_datetime = datetime.fromisoformat(element['eta'])
                current_timestamp = datetime.now()
                client_obj: Client = element['client_obj']
                if client_obj and client_obj.chat_id:
                    chat_id = client_obj.chat_id
                else:
                    stack.pop[0]   # the stack element is faulty - chat_id is missing, delete the element
                    logging.warning("DepositStack.process_next(): faulty client_obj; either missing the client_obj or the client_obj.chat_id")
                    return # return to calling function
                
                # Check if the deposit request has been sent to the client
                if not element['sent_to_client']:
                    # Check if it's time to notify the client
                    if start_datetime <= current_timestamp:
                        message = (
                            f"<b><u>ℹ️ Make a Deposit:</u></b>\n\n"
                            f"💳  Please make your deposit to this address:\n\n"
                            f"<code>{element['deposit_address']}</code>\n\n"
                            f"The deposit <b><u>minimum amount is USDT {CONFIG.DEPOSIT_MINIMUM}</u></b>.\n\n"
                            f"<b>Please make sure that you send the USDT on the POLYGON (MATIC) Network.</b>\n\n"
                            f"The time remaining to complete your deposit is {CONFIG.DEPOSIT_ADDR_VALIDITY // 60} minutes and {CONFIG.DEPOSIT_ADDR_VALIDITY % 60} seconds.\n\n"
                            f"You will be automatically notified once the deposit has been credited to our account."
                        )
                        await self.bot_message(chat_id, message)
                        element['sent_to_client'] = True
                else:
                    # Calculate remaining time until timeout
                    time_remaining = eta_datetime - current_timestamp
                    if current_timestamp < eta_datetime:
                        # Total duration of the deposit window
                        deposit_window_duration = (eta_datetime - start_datetime).total_seconds()

                        # How much time has passed since the start of the window
                        time_elapsed_since_start = (current_timestamp - start_datetime).total_seconds()

                        # Calculate the modulus to determine if it's time to send another reminder
                        if time_elapsed_since_start % CONFIG.ONGOING_DEPOSIT_REQUEST_NOTIFICATION_INTERVAL < CONFIG.DEPOSIT_REQUEST_STACK_INTERVAL:
                            # Send the periodic reminder
                            message = (
                                f"⏳ <b>Deposit Reminder:</b>\n\n"
                                f"Please note you have {time_remaining.seconds // 60} minutes "
                                f"left to complete the deposit to address <code>{element['deposit_address']}</code>."
                            )
                            await self.bot_message(chat_id, message)
                    else:
                        # If the time has expired, send a timeout message and remove the request
                        message = (
                            f"⚠️ <b><u>Deposit Timeout:</u></b>\n\n"
                            f"Your window to make a deposit using the address {element['deposit_address']} has timed out.\n\n"
                            f"If you still wish to make a deposit, please write <i>/start</i> and click on 'Deposit' again."
                        )
                        await self.bot_message(chat_id, message)
                        stack.pop(0)


        except Exception as e:
            logger.error(f"Error occurred while processing next deposit request: {e}")


    # Define a function to check if a character is Arabic
    def is_arabic(self, character):
        return re.match(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]', character)


    async def receive_deposit(self, response_data):
        """Process recent deposits and match them with the deposit requests in the stacks."""
        logger.info("Processing recent deposits.")
        
        try:
            deposits = response_data
            

            for deposit in deposits:
                deposit_address = deposit['deposit_address']
                asset = deposit['asset']
                txid = deposit['txid']
                amount = deposit['amount']
                refid = deposit['refid']  # Unique identifier of the deposit transaction
                credit_time =datetime.now()
                
                # Check if the deposit has already been processed
                if self.database.check_if_deposit_processed(refid):
                    continue # jump to next item in deposits and don't process the current one cos it's already processed
                
                # Iterate through each stack to find matching deposit requests
                for stack in self.stacks:
                    for i, request in enumerate(stack):
                        # Convert request['eta'] from ISO string to a datetime object
                        etd_datetime = datetime.fromisoformat(request['etd'])
                        eta_datetime = datetime.fromisoformat(request['eta'])
                        print(f"eta_datetime {etd_datetime} <= credit_time {credit_time}\nand eta_datetime {eta_datetime} >= credit_time {credit_time}")
                        if request['deposit_address'] == deposit_address and request['sent_to_client'] and etd_datetime <= credit_time and eta_datetime >= credit_time:
                            client_obj: Client = request['client_obj']
                            first_name = client_obj.firstname
                            last_name = client_obj.lastname
                            chat_id = client_obj.chat_id
                            
                            # Log the deposit received information
                            referral = request['referral']
                            logger.info(f"Deposit received to deposit address {deposit_address} from client {first_name} {last_name}. Amount: {amount}. Referral Code: {request['referral']}")
                            
                            # avoid 'None' in firstname or lastname and replace with empty string ""                
                            if first_name == None:
                                first_name = ""
                            if last_name == None:
                                last_name = ""
                            
                            # Add deposit record to the database to prevent re-processing
                            self.database.add_deposit_record(refid, chat_id, first_name, last_name, amount, asset, txid, deposit_address)
                            # inform communit on group chat about someone just made an investment deposit
                            if first_name != "" and first_name is not None:
                                if len(first_name) > 1:
                                    notification_username = f'{first_name[0]}{"*" * (len(first_name) - 1)}'.strip()
                                else:
                                    notification_username = first_name.strip()  # Single character remains unchanged
                            else:
                                notification_username = "default_username"

                    
                            self.database.send_deposit_notification(username=notification_username, deposit_amount=amount)
############################ UPDATE CLIENT BALANCES REMOTE PROCEDURE CALL ##################################################
                            # Update client balances and create ledger entry 
                            # Prepare data to send in the API request
                            # Convert amount to float if it's a Decimal
                            if isinstance(amount, Decimal):
                                amount = float(amount)
                            credit_time_str = credit_time.isoformat()
                            payload = {
                                "chat_id": chat_id,
                                "firstname": first_name,
                                "lastname": last_name,
                                "currency": CONFIG.ASSET,
                                "method": CONFIG.METHOD,
                                "amount": amount,
                                "deposit_address": deposit_address,
                                "kraken_refid": refid,
                                "kraken_time": credit_time_str,
                                "kraken_txid": txid,
                                "deposit_fee": CONFIG.FEES.DEPOSIT_FEE,
                                "referral": referral,
                                "referee_discount": CONFIG.FEES.REFEREE_DEPOSIT_FEE_DISCOUNT,
                                "multiplier": request['multiplier'] 
                            }

                            try:
                                # Make the API call to handle the deposit
                                response = requests.post(f"{CONFIG.RETURNS_API.APPSERVER_URL}{CONFIG.RETURNS_API.HANDLE_DEPOSIT}", json=payload)
                                response.raise_for_status()  # Raise an exception for HTTP errors
                                result = response.json()
                                logger.info(f"Deposit handled successfully: {result}")
  
                            except requests.HTTPError as e:
                                # Log the HTTP error
                                logger.error(f"HTTP error occurred: {e}")
                                
                                # Log the response content if it contains a JSON error message
                                try:
                                    error_details = response.json()  # Attempt to parse the response as JSON
                                    logger.error(f"Error details from API: {error_details}")
                                except ValueError:
                                    # If response is not JSON, log the raw content
                                    logger.error(f"Response content: {response.content.decode('utf-8')}")

                            except Exception as e:
                                logger.error(f"Error occurred while calling handle_deposit API: {e}")

#############################################################################################################################


                            # Notify client about the deposit confirmation
                            # Construct message for deposit confirmation
                            print("\n\n\n***************************************************")
                            print("***************************************************\n\n\n")
                            print("self.database.get_total_deposits_client next!")
                            print("chat_id:", chat_id, "\n\n")

                            print("***************************************************\n\n\n")
                            total_deposit_amount = self.database.get_total_deposits_client(p_chat_id=int(chat_id))
                            gross_total_deposit_amount = total_deposit_amount / (100 - CONFIG.FEES.DEPOSIT_FEE) * 100
                            print("gross_total_deposit_amount: ", gross_total_deposit_amount)
                            if gross_total_deposit_amount < (CONFIG.DEPOSIT_MINIMUM * 0.97):     # tolerance of 3% (100-97 = 3)
                                difference = CONFIG.DEPOSIT_MINIMUM - gross_total_deposit_amount
                                top_up_warning = f"\n\n❗ WARNING: The minimum deposit is USDT {CONFIG.DEPOSIT_MINIMUM}, but your deposit total is USDT {gross_total_deposit_amount}. Please add USDT {difference} to meet the minimum required for your investment to generate returns. You can make an additional deposit using the /deposit command."
                            else:
                                top_up_warning = ""

                            if referral:
                                print(f"depositstack.py - receive_deposit() - referral: {referral}")
                                if not referral.startswith('!bonuscode?'):
                                    savings = amount * (CONFIG.FEES.REFEREE_DEPOSIT_FEE_DISCOUNT / 100)
                                    message = (
                                        f"<b><u>ℹ️ Deposit Receipt Confirmation:</u></b>\n\n"
                                        f"Hello {first_name} {last_name},\n"
                                        f"🏦 Your deposit of <b>USDT {amount}</b> has been successfully received and credited to your account. "
                                        f"We are pleased to inform you that your referral code was accepted, "
                                        f"reducing the deposit fee from {CONFIG.FEES.DEPOSIT_FEE}% to "
                                        f"{CONFIG.FEES.DEPOSIT_FEE - CONFIG.FEES.REFEREE_DEPOSIT_FEE_DISCOUNT}%. "
                                        f"This means you saved USDT {savings:.6f}.\n\n"
                                        f"Thank you for your trust and welcome on board!.\nYou can check your balance anytime with the /balance command."
                                        f"{top_up_warning}"
                                    )
                                    bonus_to_referrer = amount / 100 * CONFIG.FEES.REFERRER_KICKBACK     
                                    referrer_chat_id = self.database.validate_referral(referral)
                                    try:
                                        self.database.handle_referral_bonus(p_chat_id=referrer_chat_id, p_bonus_amount=bonus_to_referrer)
                                    except Exception as e:
                                        logger.error(f"receive_deposit() - error in calling handle_referral_bonus: {e}")
                                    message_to_referrer = (
                                        "🎁 <b><u>REFERRAL BONUS PAYOUT</u></b> 🎁\n\n"
                                        f"Client {first_name} made a deposit of USDT {amount} using your referral code '{referral}'.\n"
                                        f"This earned you a bonus of <b>USDT {bonus_to_referrer:.6f}</b> which was credited to your account.\n\n"
                                        "Please check your new balance with the /balance command."
                                    )
                                    try:
                                        await self.bot.send_message(chat_id=referrer_chat_id, text=message_to_referrer, parse_mode='HTML')
                                    except Exception as e:
                                        logger.error(f"receive_deposit() - attempt to send Telegram bot message to referrer failed: {e}")
                                else:
                                    bonus_code = referral.replace('!bonuscode?','')
                                    original_deposit_amount = amount
                                    multiplier = request['multiplier']
                                    bonus_percentage = round((multiplier - 1) * 100)
                                    bonus_amount = original_deposit_amount * 0.01 * bonus_percentage
                                    total_gross_amount = amount * multiplier
                                    fee = total_gross_amount * 0.01 * CONFIG.FEES.DEPOSIT_FEE
                                    credited = total_gross_amount - fee
                                    message = (
                                        f"<b><u>ℹ️ Deposit Receipt Confirmation:</u></b>\n\n"
                                        f"Hello {first_name} {last_name},\n"
                                        f"🏦 Your deposit of <b>USDT {amount}</b> has been successfully received and credited to your account.\n\n"
                                        f"You were using bonus code {bonus_code}.\n\n"
                                        f"<code>"
                                        f"Deposit:  {original_deposit_amount:.6f}\n"
                                        f"Bonus:   +{bonus_amount:.6f} ({bonus_percentage}%)\n"
                                        f"          -------------------------------\n"
                                        f"Gross:    {total_gross_amount:.6f} USDT\n"
                                        f"Fee:     -{fee:.6f} ({CONFIG.FEES.DEPOSIT_FEE}%)\n"
                                        f"          -------------------------------\n"
                                        f"Credited: {credited:.6f} USDT\n"
                                        f"          ===============================\n"
                                        f"</code>\n\n"
                                        f"Thank you for your trust and welcome on board!.\nYou can check your balance anytime with the /balance command."
                                        f"{top_up_warning}"
                                    )
                            else:
                                fee = amount * 0.01 * CONFIG.FEES.DEPOSIT_FEE
                                credited = amount - fee
                                message = (
                                    f"<b><u>ℹ️ Deposit Receipt Confirmation:</u></b>\n\n"
                                    f"Hello {first_name} {last_name},\n"
                                    f"🏦 Your deposit has been successfully received.\n\n"
                                    f"<code>"
                                    f"Deposit:  {amount:.6f}\n"
                                    f"Fee:     -{fee:.6f} ({CONFIG.FEES.DEPOSIT_FEE}%)\n"
                                    f"          -------------------------------\n"
                                    f"Credited: {credited:.6f} USDT\n"
                                    f"          ===============================\n\n"
                                    f"</code>"
                                    f"Thank you for your trust and welcome on board!.\nYou can check your balance anytime with the /balance command."
                                    f"{top_up_warning}"
                                )

                            # use the below variant to use the automatic queuing feature
                            try:
                                await self.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
                            except Exception as e:
                                logger.error(f"receive_deposit() - attempt to send Telegram bot message to client failed: {e}")

                            if CONFIG.ADMIN_DEPOSIT_NOTIFICATION:
                                message = (
                                    f"<b>⟠⟠⟠ NEW DEPOSIT ARRIVAL ⟠⟠⟠</b>\n"
                                    f"TG user with chat-ID {chat_id}, {self.smart_concat(first_name, last_name)} just made a deposit of {CONFIG.ASSET} {amount}"
                                )
                                for admin_chat_id in CONFIG.ADMIN_CHAT_IDS:
                                    try:
                                        await self.bot.send_message(chat_id=admin_chat_id, text=message, parse_mode='HTML')
                                    except Exception as e:
                                        error_message =f"Error occured sending admin notifications: {str(e), admin_chat_id}"
                                        logger.error(error_message)

                            # Add refid to known refids to avoid processing it again
                            self.deposit_ref_ids.add(refid)
                            
                            # Remove the processed request from the stack
                            stack.pop(i)
                            break  # Exit the loop after processing the deposit request
                            
        except Exception as e:
            logger.error(f"Error occurred while processing recent deposits: {e}")
            
    

    def smart_concat(self, str1, str2):
        if str1 and str2:
            return f"{str1} {str2}"
        elif str1:
            return str1
        elif str2:
            return str2
    
    def __len__(self):
        """Return the total number of deposit requests across all stacks."""
        try:
            # Sum up the lengths of all stacks to get the total number of deposit requests
            total_requests = sum(len(stack) for stack in self.stacks)
            return total_requests
        except Exception as e:
            # Log any errors that occur during the calculation
            logger.error(f"Error occurred while calculating total number of deposit requests: {e}")
            


    def __repr__(self):
        """Return a string representation of the DepositStack object."""
        try:
            # Return a string representation that includes the stacks attribute
            return f"DepositStack(stack={self.stacks})"
        except Exception as e:
            # Log any errors that occur during the representation creation
            logger.error(f"Error occurred while creating string representation of DepositStack: {e}")
            
