# fastapi_app/fastapi_app.py

# Import necessary modules from FastAPI and Pydantic
from telegram.ext import Application
import requests
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from config import CONFIG

# Import datetime module for handling date and time operations
from datetime import datetime

# Import custom modules or classes
from model import DataHandler

# Import the singleton application object
from telegram_bot import application

from .utils import Utils
import logging

# Initialize FastAPI application
app = FastAPI()


# The above imports and initialization serve as the foundation for the FastAPI application.
# Each import has a specific purpose:

# 1. FastAPI: Core library to create the API endpoints and handle requests/responses.
# 2. HTTPException: Used to raise HTTP exceptions with custom status codes and detail messages.
# 3. Body: Utility function to extract and validate the request body parameters.
# 4. BaseModel: Pydantic's base class for creating data models with validation.
# 5. datetime: Standard library for date and time manipulation.
# 6. DataHandler: Custom module for data handling (ensure the module and class exist and are correctly implemented).
# 7. Utils: Custom utility functions (ensure the module and class exist and are correctly implemented).

# Handle potential import errors with try-except blocks for custom modules
try:
    from model import DataHandler
except ImportError as e:
    raise ImportError(f"Failed to import DataHandler from model module: {e}")

try:
    from .utils import Utils
except ImportError as e:
    raise ImportError(f"Failed to import Utils from utils module: {e}")

# The app object is an instance of the FastAPI class and will be used to define the API endpoints.



class ApprovedWithdrawal(BaseModel):
    """
    Pydantic model for representing an approved withdrawal.

    Attributes:
        wrid (int): Withdrawal request ID.
        chat_id (int): Chat ID of the user.
        firstname (str): First name of the user.
        lastname (str): Last name of the user.
        currency (str): Currency type for the withdrawal.
        amount (float): Total amount requested for withdrawal.
        net_amount (float): Net amount after deducting fees.
        fee_percent (float): Percentage of the fee applied.
        fee_amount (float): Amount of fee deducted.
        wallet (str): Wallet address to which the amount will be withdrawn.
        timestamp (datetime): Timestamp when the withdrawal was approved.
        status (str): Status of the withdrawal.
        approved_by_username (str): Username of the admin who approved the withdrawal.
    """

    wrid: int
    chat_id: int
    firstname: str
    lastname: str
    currency: str
    amount: float
    net_amount: float
    fee_percent: float
    fee_amount: float
    wallet: str
    timestamp: datetime
    status: str
    approved_by_username: str


class DeclinedWithdrawal(BaseModel):
    """
    Pydantic model for representing a declined withdrawal.

    Attributes:
        wrid (int): Withdrawal request ID.
        chat_id (int): Chat ID of the user.
        firstname (str): First name of the user.
        lastname (str): Last name of the user.
        currency (str): Currency type for the withdrawal.
        amount (float): Total amount requested for withdrawal.
        net_amount (float): Net amount after deducting fees.
        fee_percent (float): Percentage of the fee applied.
        fee_amount (float): Amount of fee deducted.
        wallet (str): Wallet address to which the amount was intended to be withdrawn.
        timestamp (datetime): Timestamp when the withdrawal was declined.
        status (str): Status of the withdrawal.
        declined_by_username (str): Username of the admin who declined the withdrawal.
    """

    wrid: int
    chat_id: int
    firstname: str
    lastname: str
    currency: str
    amount: float
    net_amount: float
    fee_percent: float
    fee_amount: float
    wallet: str
    timestamp: datetime
    status: str
    declined_by_username: str


class RollbackWithdrawalData(BaseModel):
    """
    Pydantic model for representing data needed to rollback an approved withdrawal.

    Attributes:
        wrid (int): Withdrawal request ID.
        chat_id (str): Chat ID of the user.
        firstname (str): First name of the user.
        lastname (str): Last name of the user.
        currency (str): Currency type for the withdrawal.
        amount (float): Total amount requested for withdrawal.
        net_amount (float): Net amount after deducting fees.
        fee_percent (float): Percentage of the fee applied.
        fee_amount (float): Amount of fee deducted.
        wallet (str): Wallet address to which the amount was intended to be withdrawn.
        timestamp (datetime): Timestamp when the withdrawal was created.
        approved_timestamp (datetime): Timestamp when the withdrawal was approved.
        status (str): Status of the withdrawal.
        approved_by (int): ID of the admin who approved the withdrawal.
    """

    wrid: int
    chat_id: str
    firstname: str
    lastname: str
    currency: str
    amount: float
    net_amount: float
    fee_percent: float
    fee_amount: float
    wallet: str
    timestamp: datetime
    approved_timestamp: datetime
    status: str
    approved_by: int


class UpdateDepositLogsRefundRequest(BaseModel):
    """
    Pydantic model for representing the request body for updating deposit logs with refund data.

    Attributes:
        p_id (str): The ID of the deposit log entry.
        p_refund_transaction_id (str): The refund transaction ID to be associated with the deposit log entry.
    """
    p_transaction_id: str
    p_refund_transaction_id: str    


@app.post("/api/approved_withdrawal")
async def handle_approved_withdrawal(data: ApprovedWithdrawal = Body(...)):
    """
    Endpoint to handle approved withdrawals.

    Args:
        data (ApprovedWithdrawal): The approved withdrawal data sent in the request body.

    Returns:
        dict: A success message if the withdrawal was processed correctly.
    
    Raises:
        HTTPException: If any error occurs during processing the withdrawal.
    """
    print("\n\n\n")
    print("**********************************************")
    print("\n /api/approved_withdrawal\n")
    print("**********************************************")
    print("\n\n\n")
    try:
        # Extract data from the request
        wrid = data.wrid
        chat_id = data.chat_id
        firstname = data.firstname
        lastname = data.lastname
        currency = data.currency
        amount = data.amount
        net_amount = data.net_amount
        fee_percent = data.fee_percent
        fee_amount = data.fee_amount
        wallet = data.wallet
        timestamp = data.timestamp
        status = 'Approved'
        approved_by_username = data.approved_by_username

        # Log the received data
        logging.info(f"Approved withdrawal for {firstname} {lastname}:")
        logging.info(f"- WRID: {wrid}")
        logging.info(f"- Chat ID: {chat_id}")
        logging.info(f"- Currency: {currency}")
        logging.info(f"- Amount: {amount}")
        logging.info(f"- Net Amount: {net_amount}")
        logging.info(f"- Fee Percent: {fee_percent}")
        logging.info(f"- Fee Amount: {fee_amount}")
        logging.info(f"- Wallet: {wallet}")
        logging.info(f"- Timestamp: {timestamp}")
        logging.info(f"- Status: {status}")
        logging.info(f"- Approved by: {approved_by_username}")

        # Update balance in the database
        # database = DataHandler()
        # database.withdraw(chat_id, amount)

        # Prepare the request payload
        payload = {
            "chat_id": chat_id,
            "amount": amount
        }

        # Make the HTTP request to the withdraw endpoint
        # this updates ledger and balance in the Returns app database
        withdraw_url = f"{CONFIG.RETURNS_API.APPSERVER_URL}{CONFIG.RETURNS_API.WITHDRAW}"
        response = requests.post(withdraw_url, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Create and send a confirmation message
        message = (
            f"<b>💰 Withdrawal Approval Confirmation</b>\n\n"
            f"<code>Requested by user...: {firstname} {lastname}\n"
            f"Requested amount....: {currency} {amount}\n"
            f"Fee in percent......: {fee_percent}\n"
            f"Fee amount..........: {fee_amount}\n"
            f"<b>Payout net amount...: {net_amount}</b>\n"
            f"Beneficiary wallet..: {wallet}\n"
            f"Status..............: {status}</code>\n"
        )
        await Utils.bot_message(chat_id, message, application)

        # Return a success message
        return {"status": "success", "message": "Approved withdrawal processed"}

    except Exception as e:
        logging.error(f"Error processing approved withdrawal: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing approved withdrawal")


@app.post("/api/declined_withdrawal")
async def handle_declined_withdrawal(data: DeclinedWithdrawal = Body(...)):
    """
    Endpoint to handle declined withdrawals.

    Args:
        data (DeclineddWithdrawal): The declined withdrawal data sent in the request body.

    Returns:
        dict: A success message if the withdrawal was processed correctly.

    Raises:
        HTTPException: If any error occurs during processing the withdrawal.
    """
    try:
        # Extract data from the request
        wrid = data.wrid
        chat_id = data.chat_id
        firstname = data.firstname
        lastname = data.lastname
        currency = data.currency
        amount = data.amount
        net_amount = data.net_amount
        fee_percent = data.fee_percent
        fee_amount = data.fee_amount
        wallet = data.wallet
        timestamp = data.timestamp
        status = 'Declined'
        declined_by_username = data.declined_by_username

        # Log the received data
        logging.info(f"Declined withdrawal for {firstname} {lastname}:")
        logging.info(f"- WRID: {wrid}")
        logging.info(f"- Chat ID: {chat_id}")
        logging.info(f"- Currency: {currency}")
        logging.info(f"- Amount: {amount}")
        logging.info(f"- Net Amount: {net_amount}")
        logging.info(f"- Fee Percent: {fee_percent}")
        logging.info(f"- Fee Amount: {fee_amount}")
        logging.info(f"- Wallet: {wallet}")
        logging.info(f"- Timestamp: {timestamp}")
        logging.info(f"- Status: DECLINED")
        logging.info(f"- Declined by: {declined_by_username}")

        # Create and send a decline message
        message = (
            f"<b>🚫💰🚫 Withdrawal Declined 🚫💰🚫</b>\n\n"
            f"<code>Requested by user...: {firstname} {lastname}\n"
            f"Requested amount....: {currency} {amount}\n"
            f"Fee in percent......: {fee_percent}\n"
            f"Fee amount..........: {fee_amount}\n"
            f"<b>Payout net amount...: {net_amount}</b>\n"
            f"Beneficiary wallet..: {wallet}\n"
            f"Status..............: DECLINED</code>\n"
            "A withdrawal request may be declined if the requested amount "
            "exceeds the balance. Please verify your balance using /balance.\n"
            "To create a new withdrawal request, use /withdraw."
        )
        await Utils.bot_message(chat_id, message, application)

        # Return a success message
        return {"status": "success", "message": "Declined withdrawal processed"}

    except Exception as e:
        logging.error(f"Error processing declined withdrawal: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing declined withdrawal")


@app.post("/api/balance_rollback")
async def balance_rollback(data: RollbackWithdrawalData = Body(...)):
    """
    Endpoint to handle balance rollback requests.

    Args:
        data (RollbackWithdrawalData): The rollback withdrawal data sent in the request body.

    Returns:
        dict: A success message if the rollback was processed correctly.

    Raises:
        HTTPException: If any error occurs during processing the rollback.
    """
    try:
        # Extract data from the request
        wrid = data.wrid
        transaction_type = "Correction"
        chat_id = data.chat_id
        firstname = data.firstname
        lastname = data.lastname
        currency = data.currency
        method = None
        amount = data.amount
        deposit_address = None
        kraken_refid = None
        kraken_time = None
        kraken_txid = None
        target_address = data.wallet

        # Log the received data
        logging.info(f"Balance rollback request for {firstname} {lastname}:")
        logging.info(f"- WRID: {wrid}")
        logging.info(f"- Chat ID: {chat_id}")
        logging.info(f"- Currency: {currency}")
        logging.info(f"- Amount: {amount}")
        logging.info(f"- Target Address: {target_address}")

        # Update the balance in the database
        # database = DataHandler()
        # database.correct_balance(chat_id, amount)

        # Define the URL for the rollback endpoint
        rollback_url = f"{CONFIG.RETURNS_API.APPSERVER_URL}{CONFIG.RETURNS_API.ROLLBACK_WITHDRAWAL}"

        # Make the request to the Returns app
        response = requests.post(rollback_url, json={"chat_id": chat_id, "amount": amount})
        response.raise_for_status()  # Raise an HTTPError if the response status is 4xx or 5xx

        # Create and send a confirmation message
        message = (
            f"<b>🔄 Rollback of Approved Withdrawal by Administrator 🔄</b>\n\n"
            f"<code>Requested by user...: {firstname} {lastname}\n"
            f"Requested amount....: {currency} {amount}\n\n"
            f"The previously granted approval was withdrawn by the Administrator."
            f"Your withdrawal request is now pending for approval again.</code>"

        )
        await Utils.bot_message(chat_id, message, application)

        # Return a success message
        return {"status": "success", "message": "Balance rollback processed"}

    except Exception as e:
        logging.error(f"Error processing balance rollback: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing balance rollback")
    

@app.get("/api/get_unidentified_deposits")
async def get_unidentified_deposits():
    """
    Endpoint to retrieve all unidentified deposits from the database.

    Returns:
        list of dict: A list of dictionaries, each representing a row in the depositlogs_view.

    Raises:
        HTTPException: If any error occurs during retrieval.
    """
    try:
        # Instantiate DataHandler
        data_handler = DataHandler()

        # Call the method to retrieve deposits
        deposits_list = data_handler.get_unidentified_deposits()

        # Return the list of dictionaries as JSON
        return deposits_list

    except Exception as e:
        logging.error(f"Error retrieving unidentified deposits: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving unidentified deposits")

@app.post("/api/update_depositlogs_refund")
async def update_depositlogs_refund(data: UpdateDepositLogsRefundRequest = Body(...)):
    """
    Endpoint to update deposit logs with refund data.

    Args:
        data (UpdateDepositLogsRefundRequest): The data for updating the deposit log entry with refund details.

    Returns:
        dict: A success message if the deposit log was updated correctly.

    Raises:
        HTTPException: If any error occurs during the update process.
    """
    try:
        # Extract data from the request
        p_transaction_id = data.p_transaction_id
        p_refund_transaction_id = data.p_refund_transaction_id

        # we need to add single quotes as IDs are stored with single-quotes in db.
        p_transaction_id = f"'{p_transaction_id}'"
        print(f"\n\n\np_transaction_id: {p_transaction_id}\np_refund_transaction_id: {p_refund_transaction_id}\n\n\n")

        # Log the received data
        logging.info(f"Updating deposit log with ID: {p_transaction_id}")
        logging.info(f"- Refund Transaction ID: {p_refund_transaction_id}")

        # Instantiate the DataHandler and call the update_depositlogs_refund method
        database = DataHandler()
        database.update_depositlogs_refund(p_transaction_id, p_refund_transaction_id)

        # Return a success message
        return {"status": "success", "message": "Deposit log updated with refund data"}

    except Exception as e:
        logging.error(f"Error updating deposit log with refund data: {str(e)}")
        raise HTTPException(status_code=500, detail="Error updating deposit log with refund data")




if __name__ == "__main__":
    # Import uvicorn only if running this script directly
    import uvicorn

    # Run the FastAPI application using uvicorn server
    uvicorn.run(app, host="localhost", port=8000)
    # 'app' is the FastAPI application instance to run
    # 'host' specifies the server address to listen on (localhost in this case)
    # 'port' specifies the port number to listen on (8000 in this case)
