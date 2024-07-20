import json
import requests
import threading
import logging
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, CallbackContext, ConversationHandler
from datetime import datetime
from config import CONFIG
from model import DataHandler
from krakenapi import KrakenAPI
from client import Client
from depositstack import DepositStack
from withdraw_data import ClientWithdrawal
from bot_workflows import Workflows
from testunit import TestUnit

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
active_chats = []
database = DataHandler()
depositstack = DepositStack(database)
withdrawals = ClientWithdrawal()
testunit = TestUnit() # only for testing, remove for production



# Command handlers
async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text('Welcome to the USDT TRC20 Bot!')
    await startmenu(update, context)


async def startmenu(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    img = CONFIG.LOGO_PATH
    message = (
        f"<b>Hello {user.first_name}!\nWelcome to OrcaTrade.</b>\n\n"
        "Please choose an option from the menu below:"
    )
    print("Workflow: ", Workflows.RequestDeposit.SDA_0['function'])
    keyboard = [
        [InlineKeyboardButton("Deposit\u2003\u2003\u2003\u2003💳", callback_data=json.dumps(
            {
                "status": "request_deposit",
                "decision": "",
            }))],
        [InlineKeyboardButton("Balance\u2003\u2003\u2003🏦", callback_data=Workflows.GetBalance.GEB_0['function'])],
        [InlineKeyboardButton("Withdraw\u2003💰", callback_data=Workflows.Withdraw.WDR_0['function'])],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    with open(img, 'rb') as imgt:
        await update.message.reply_photo(photo=img, caption=message, parse_mode='HTML', reply_markup=reply_markup)


async def enter_address(update: Update, context: CallbackContext) -> int:
    user_data = context.user_data
    user_data['address'] = update.message.text

    user = update.message.from_user
    amount = user_data['amount']
    address = user_data['address']

    admin_message = f'User {user.username} requested withdrawal of {amount} TRX to address {address}.'
    await context.bot.send_message(chat_id=CONFIG.ADMIN_CHAT_ID, text=admin_message)
    await update.message.reply_text(f'Your withdrawal request has been sent to the admin.')

    user_data.clear()

    return ConversationHandler.END


async def help(update: Update, context: CallbackContext) -> None:
    bot_commands = "<b><u>List of bot commands:</u></b>\n\n"
    bot_commands += "🚀  /start:\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003Starts the bot\n\n"
    bot_commands += "💳  /deposit:\u2003\u2003\u2003\u2003\u2003Make deposit\n\n"
    bot_commands += "🏦  /balance:\u2003\u2003\u2003\u2003\u2003Your account balance\n\n"
    bot_commands += "💰  /withdraw:\u2003\u2003Request withdraw\n\n"
    bot_commands += "❓  /help:\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003\u2003Displays this help\n\n"

    await update.message.reply_text(bot_commands, parse_mode='HTML')


async def handle_message(update: Update, context: CallbackContext) -> None:
    if context.user_data.get('amount') is None or context.user_data.get('address') is None:
        text = update.message.text
        await update.message.reply_text(f'You said: {text}')


def add_chat(update: Update, chat_id):
    chat_id = update.message.chat_id
    firstname = update.effective_user.first_name
    lastname = update.effective_user.last_name
    language = update.effective_user.language_code
    user_object = Client(chat_id=chat_id,
                         firstname=firstname,
                         lastname=lastname,
                         lang=language,
                         status=Workflows.Idle.IDLE_0)
    
    timestamp = datetime.now()

    new_tguser = {
        'chat_id': chat_id,
        'user_object': user_object,
        'create_time': timestamp,
        'last_accessed': timestamp
    }
    active_chats.append(new_tguser)
    print(active_chats)
    return new_tguser


def get_client_object(update: Update, chat_id) -> Client:
    for client in active_chats:
        if client['chat_id'] == chat_id:
            client_object = client['user_object']
            return client_object
    return None


def check_chat(update, chat_id):
    for tguser in active_chats:
        if tguser['chat_id'] == chat_id:
            timestamp = datetime.now()
            tguser['last_accessed'] = timestamp
            print(tguser)
            return tguser
    return add_chat(update, chat_id)


async def send_message(message_obj, context: CallbackContext, message: str) -> None:
    await message_obj.reply_text(message, parse_mode='HTML')


async def show_deposit_address(update: Update, context: CallbackContext, chat_id):
    client: Client = get_client_object(update, chat_id)
    # is it ordinary chat or callback
    if update.message:                # ordinary chat
        message_obj = update.message
    elif update.callback_query:       # callback
        message_obj = update.callback_query.message

    if client.active_deposit_address:
        message = f"⚠️ <b>You already requested a deposit.</b>\n\n"
        message += f"Please send your deposit to following address:\n"
        message += f"<code>{client.active_deposit_address}</code>\n\n"
        message += f"You will be automatically notified, once the deposit has been credited to our account."
        await send_message(message_obj, context, message)
        return

    text = "preparing deposit address..."


    await message_obj.reply_text(f'{text}')

    api = KrakenAPI(CONFIG.SPOT_API_KEY, CONFIG.SPOT_PRIVATE_KEY)
    response_data = api.generate_new_deposit_address(
        asset=CONFIG.ASSET,
        method=CONFIG.METHOD,
        new=False
    )
    deposit_addresses = response_data.get('result', [])

    in_use = False
    for deposit_address in deposit_addresses:
        for chat in active_chats:
            client_object: Client = chat['user_object']
            print("User Object: ", client_object.firstname, client_object.chat_id, client_object.active_deposit_address)
            if client_object.active_deposit_address:
                if client_object.active_deposit_address == deposit_address['address']:
                    print(f"address {deposit_address['address']} is already in use!")
                    in_use = True
                    break
        if in_use == False:
            client.active_deposit_address = deposit_address['address']
            break

        if in_use == True:
            in_use = False


    print("\n\nCLIENTS:\n")
    for chat in active_chats:
        print(chat)
        client_obj = chat['user_object']
        print("ACTIVE DEPOSIT ADDRESS:", client_obj.active_deposit_address)
    bot_text = "<b><u>Make a Deposit:</u></b>\n\n"
    bot_text += "💳  Please make your deposit to this address:\n\n" + f"<code>{client.active_deposit_address}</code>\n"
    bot_text += " \n"
    bot_text += "You will be automatically notified, once the deposit has been credited to our account."

    await message_obj.reply_text(bot_text, parse_mode='HTML')
    


async def show_balance(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    result = database.get_balance(chat_id)
    if result:
        balance_info = result[0]
        print("########### RESULT: ", result)
        firstname = balance_info['firstname']
        lastname = balance_info['lastname']
        currency = balance_info['currency']
        balance = balance_info['balance']
        last_update_date = balance_info['last_update_date']

    bot_text = f"ℹ️ Client: {firstname} {lastname}\nBalance: {currency} {balance}\nLast updated: {last_update_date}"
    print(f"\n\nBOT TEXT: {bot_text}\n\n\n")
    await update.message.reply_text(bot_text, parse_mode='HTML')


async def request_withdrawal(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    context.user_data['status'] = 'withdrawal: awaiting amount'
    bot_text = f"Enter withdrawal amount:"
    await update.message.reply_text(bot_text, parse_mode='HTML')


async def client_commit_to_deposit(chat_id, context: CallbackContext):
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
    text = f"❓ Make a Deposit\n\nDue to high demand, the deposit address necessary to make a deposit will only be reserved for {value} {unit}.\n\n Please confirm if you are ready to make the deposit now."
    reply_markup = InlineKeyboardMarkup(keyboard_options)
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')


async def client_confirm_withdrawal(chat_id, context: CallbackContext):
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
    withdrawal = withdrawals.get_withdrawal_data(chat_id)
    print(f"\n\n\n\nRAW WITHDRAWAL DATA: {withdrawal}\n\n\n\n")
    text = f"❓ Withdraw funds\n\nRequested amount: USDT {withdrawal['amount']}\nBeneficiary wallet: {withdrawal['wallet']}\n\n Please confirm with 'confirm' to proceed or 'cancel' to cancel the withdrawal."
    reply_markup = InlineKeyboardMarkup(keyboard_options)
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')


async def admin_confirm_payout(chat_id, chat_id_client, amount, context: CallbackContext):
    keyboard_options = [
        [
            InlineKeyboardButton(
                "confirm",
                callback_data=json.dumps({
                    "status": "payout: confirm"+chat_id_client,
                    "decision": "yes",
                    })
                ),
            InlineKeyboardButton(
                "cancel",
                callback_data=json.dumps({
                    "status": "payout: confirm"+chat_id_client,
                    "decision": "no",
                    })
                )
        ]
    ]
    client = database.get_client(chat_id_client)[0]
    text = f"❓ Payout funds\n\nClient: {client['firstname']} {client['lastname']}\n\nAmount: USDT {amount}\n Please confirm with 'confirm' to proceed or 'cancel' to cancel the payout."
    reply_markup = InlineKeyboardMarkup(keyboard_options)
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')


# Command handler function to execute workflow actions
async def execute_workflow_action(update: Update, context: CallbackContext, action: str) -> None:
    if update.message:
        chat_id = update.message.chat_id
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id

    check_chat(update, chat_id)
    client=get_client_object(update, chat_id)
    print("\n\nClient DATA: \n", client.firstname, client.lastname, client.balance, client.status, client.chat_id)
    print("\n\nACTION STRING: ", action, "\n\n")
    if action == 'start':
        client.status = Workflows.Start.MENU_0
        print("CLIENT STATUS: ", client.status)
        await start(update, context)
    elif action == 'show_deposit_address':
        client.status = Workflows.RequestDeposit.SDA_0
        print("CLIENT STATUS: ", client.status)
        # await depositstack.add_deposit_request(update, client) # add the request to the deposit stack for FIFO processing
        await client_commit_to_deposit(chat_id, context)
        print(f"########## DEPOSIT STACK: \n{depositstack}\n\n\n")
    elif action == 'show_balance':
        client.status = Workflows.GetBalance.GEB_0
        await show_balance(update, context)
    elif action == 'request_withdrawal':
        client.status = Workflows.Withdraw.WDR_0
        await request_withdrawal(update, context)
    elif action == 'show_command_list':
        client.status = Workflows.Idle.IDLE_0
        print("CLIENT STATUS: ", client.status)
        await help(update, context)
    else:
        await update.message.reply_text("Invalid action requested.")


# Add a handler for callback queries
async def button(update: Update, context: CallbackContext) -> None:
    if update.message:
        chat_id = update.message.chat_id
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
    query = update.callback_query
    print("\n\nBUTTON CALLBACK UPDATE CHAT_ID: ", query.message.chat_id, "\n\n")
    callback_data_json = query.data
    if callback_data_json:
        # deserialize callback data
        callback_data = json.loads(callback_data_json)
        status = callback_data.get("status")
        decision = callback_data.get("decision")

        if status == "client_commit_to_deposit":
            if decision == "yes":
                client=get_client_object(update, chat_id)
                deposit_request = await depositstack.add_deposit_request(update, client) # add the request to the deposit stack for FIFO processing
                ######### START OF TEST-CODE #########  REMOVE BEFORE PRODUCTION USE ##########
                # TestUnit.make_payment simulates the payment so that we can test receiving deposits. 
                # This needs to be removed or commented out before transition to production.
                print(">>>>>>>>>>>>>>>> <<<<<<<<<<<<<<<<<<<<<<<<<")
                await testunit.make_payment(asset=CONFIG.ASSET, 
                                            method=CONFIG.METHOD,
                                            eta=deposit_request['eta'],
                                            deposit_address=deposit_request['deposit_address'])
                ######### END OF TEST-CODE ###########  REMOVE BEFORE PRODUCTION USE ##########
            else:
                await update.callback_query.message.reply_text("You're welcome any time to request to make a deposit.")

        elif status == "request_deposit":
            action = "show_deposit_address"
            client=get_client_object(update, chat_id)
            await execute_workflow_action(update, context, action)
        
        elif status == "withdrawal: confirm":
            if decision == "yes":
                client_raw = database.get_client(chat_id)
                client = client_raw[0]
                withdrawal = withdrawals.get_withdrawal_data(chat_id)
                amount = withdrawal['amount']
                wallet = withdrawal['wallet']
                balance_raw = database.get_balance(chat_id)
                if balance_raw:
                    balance_info = balance_raw[0]
                    balance = balance_info['balance']

                # prepare data to send sister application
                data = {
                    "chat_id": chat_id,
                    "firstname": client['firstname'],
                    "lastname": client['lastname'],
                    "currency": "USDT",
                    "amount": amount,
                    "wallet": wallet
                }
                # send post request to sister application
                url = CONFIG.ENDPOINT_BASEURL + "/request_withdrawal"
                headers = {'Content-Type': 'application/json'}
                response = requests.post(url, headers=headers, data=json.dumps(data))
                print(f"\n\n\nRESPONSE FROM INTEGRATION ENDPOINT: {response}\n\n\n")
                context.user_data['status'] = None # reset status because user process ends here
                message = f"<b>🔴 WITHDRAWAL REQUEST 💵</b>\n\nBy user: {client['firstname']} {client['lastname']}\nTelegram user-id: <code>{chat_id}</code>\nBalance USDT {balance}\nWithdrawal amount: USDT <code>{amount}</code>\nBeneficiary account: <code>{wallet}</code>"
                await depositstack.bot_message(chat_id=CONFIG.ADMIN_CHAT_ID, message=message)
                message = f"Your request to withdraw USDT {str(amount)} was forwarded to the administrator."
                await depositstack.bot_message(chat_id=chat_id, message=message)
                withdrawals.remove_withdrawal(chat_id)
            else:
                context.user_data['status'] = None # reset status because user process ends here
                withdrawals.remove_withdrawal(chat_id)
                message = "Withdrawal canceled by user."
                await depositstack.bot_message(chat_id=chat_id, message=message) # confirm cancelation to user, no message to admin


    # await query.answer()
    
    # command = query.data

    # await execute_workflow_action(update, context, command)


async def poll_deposit_request_stack():
    while True:
        next_request = await depositstack.process_next()
        await asyncio.sleep(CONFIG.DEPOSIT_REQUEST_STACK_INTERVAL)  # sleep for x seconds (defined in config.py)


async def poll_recent_deposits():
    api = KrakenAPI(CONFIG.SPOT_API_KEY, CONFIG.SPOT_PRIVATE_KEY)
    while True:
        # response_data = api.get_recent_deposits(asset=CONFIG.ASSET,
        #                                        method=CONFIG.METHOD)  # for production use only

        response_data = testunit.get_recent_deposits(asset=CONFIG.ASSET,
                                                     method=CONFIG.METHOD) # for test use only
        print("Recent Deposits: ", response_data)

        await depositstack.receive_deposit(response_data)

        # if len(response_data['result']) == 0:
        #     print("No recent deposits found.")
        # else:
        #     print("Recent deposits found:", response_data['result'])

        await asyncio.sleep(CONFIG.DEPOSIT_POLLING_INTERVAL)  # sleep for x seconds (defined in config.py)


async def handle_text_input(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    user_data = context.user_data
    if update.message.text == 'cancel' and not context.user_data['status'] == None:
        await update.message.reply_text(f'The current operation "{user_data["status"]}" was canceled.')        
        context.user_data['status'] = None
        return
    elif update.message.text == 'cancel' and context.user_data['status'] == None:
        await update.message.reply_text("Nothing to cancel: No ongoing operation.")        


    if user_data['status'] == 'withdrawal: awaiting amount':
        if update.message.text:
            amount_text = update.message.text
            try:
                amount = float(amount_text)
                context.user_data['status'] = ''
                result = database.get_balance(chat_id)
                if result:
                    balance_info = result[0]
                    balance = float(balance_info['balance'])
                    if amount > balance:
                        await update.message.reply_text(f"The requested withdrawal amount {str(amount)} exceeds your balance {str(balance)}. Please enter a lower amount or enter 'cancel' to cancel the withdrawal.")
                        context.user_data['status'] = 'withdrawal: awaiting amount'
                    else:
                        withdrawals.update_amount(chat_id, amount)
                        message = "Now, please enter your wallet address or enter 'cancel' to cancel the withdrawal:"
                        context.user_data['status']  = 'withdrawal: awaiting wallet'
                        await depositstack.bot_message(chat_id=chat_id, message=message)
            except ValueError:
                await update.message.reply_text("Please enter a valid amount or enter 'cancel' to cancel the withdrawal.")
    elif user_data['status'] == 'withdrawal: awaiting wallet':
        print('\n\n\nAWAITING WALLET!!!!\n\n\n\n')
        if update.message.text:
            wallet_text = update.message.text
            withdrawals.update_wallet(chat_id, wallet_text)
            await client_confirm_withdrawal(chat_id, context)










    print('this is the text handler')





def start_telegram_bot():
    global application
    application = Application.builder().token(CONFIG.TELEGRAM_KEY).build()

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


def main():
    # Create and start a thread for deposit polling
    thread_deposits = threading.Thread(target=poll_recent_deposits_wrapper)
    thread_deposits.start()


    # Create and start a thread for deposit request stack polling
    thread_deposit_request_stack = threading.Thread(target=poll_deposit_request_stack_wrapper)
    thread_deposit_request_stack.start()


    # Start the Telegram bot in the main thread
    start_telegram_bot()

    # Wait for the deposit polling thread to finish (though in practice it runs indefinitely)
    thread_deposits.join()


def poll_recent_deposits_wrapper():
    # Create an asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the poll_recent_deposits coroutine in the event loop
    try:
        loop.run_until_complete(poll_recent_deposits())
    finally:
        loop.close()


def poll_deposit_request_stack_wrapper():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(poll_deposit_request_stack())
    finally:
        loop.close()



if __name__ == '__main__':
    main()