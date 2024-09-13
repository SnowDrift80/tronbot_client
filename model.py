# model.py
import requests
import psycopg2
from decimal import Decimal
from psycopg2.extras import RealDictRow
import psycopg2.extras
import logging
import argparse
from config import CONFIG

DBCONF = CONFIG.DBCONFIG

class DataHandler:
    """
    Facilitates database operations for the bot commands by invoking stored procedures
    and functions with parameters and returning results back to the bot.

    Methods:
        - call_procedure(proc_name, *args): Executes a stored procedure with the given name and arguments.
        - call_function(func_name, *args): Invokes a stored function with the provided name and arguments.
        - import_csv_data(csv_path): Imports CSV data into the database using the 'import_csv_data' procedure.
        - add_deposit_record(p_refid, p_chat_id, p_firstname, p_lastname, p_amount, p_asset, p_txid, p_deposit_address):
          Adds a deposit record to the 'deposits' table.
        - check_if_deposit_processed(p_refid): Checks if a deposit with the given refid has been processed.
        - compound_returns(p_current_date): Compounds returns and updates balances based on the given date.
        - correct_balance(p_chat_id, p_amount): Corrects the balance and logs the correction in the ledger.
        - handle_deposit(p_chat_id, p_firstname, p_lastname, p_currency, p_method, p_amount, p_deposit_address,
                         p_kraken_refid, p_kraken_time, p_kraken_txid):
          Handles a deposit transaction, updates balances, and logs the transaction in the ledger.
        - get_statement(p_chat_id, p_start_date, p_end_date): Retrieves transaction statements between two dates.
        - get_total_liabilities(): Calculates the total liabilities from the 'balances' table.
        - projected_balance(p_target_date, p_chat_id): Calculates projected balance up to a target date for a client.
        - get_balance(p_chat_id): Retrieves the current balance information for a client.
        - get_client(p_chat_id): Retrieves client information based on the chat_id.

        - close(): Closes the database connection.
    """
    def __init__(self) -> None:
        """
        Initializes the DataHandler object by establishing a connection to the database.

        Raises:
            Exception: If there is an error connecting to the database.
        """
        try:
            self.conn = psycopg2.connect(
                dbname=DBCONF.DBNAME,
                user=DBCONF.USER,
                password=DBCONF.PASSWORD,
                host=DBCONF.HOST,
                port=DBCONF.PORT
            )
            self.conn.autocommit = True
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")
            raise


    def call_procedure(self, proc_name, *args):
        """
        Executes a stored procedure with the given name and arguments.

        Args:
            proc_name (str): The name of the stored procedure to execute.
            *args: Variable length argument list for procedure parameters.

        Returns:
            list: List of dictionaries containing the results of the procedure execution.
                Each dictionary represents a row of results with column names as keys.

        Raises:
            Exception: If there is an error executing the stored procedure.
        """
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Construct the CALL statement for procedures
                call_statement = f"CALL {proc_name}({', '.join('%s' for _ in args)})"
                cursor.execute(call_statement, args)
                
                # Try to fetch the results if there are any
                try:
                    result = cursor.fetchall()
                except psycopg2.ProgrammingError:
                    # No results to fetch
                    result = None
                    
                return result
            
        except Exception as e:
            logging.error(f"Error calling procedure {proc_name}: {e}")
            raise


    def call_function(self, func_name, *args):
        """
        Executes a stored function with the given name and arguments.

        Args:
            func_name (str): The name of the stored function to execute.
            *args: Variable length argument list for function parameters.

        Returns:
            list: List of dictionaries containing the results of the function execution.
                Each dictionary represents a row of results with column names as keys.

        Raises:
            Exception: If there is an error executing the stored function.
        """
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.callproc(func_name, args)
                self.conn.commit()
                result = cursor.fetchall()
                return result
        except Exception as e:
            logging.error(f"Error calling function {func_name}: {e}")
            raise



    def import_csv_data(self, csv_path):
        """
        Imports data from a CSV file into the database using a stored procedure.

        Args:
            csv_path (str): The path to the CSV file containing data to be imported.

        Returns:
            list: List of dictionaries containing any results returned by the stored procedure.
                Each dictionary represents a row of results with column names as keys.

        Raises:
            Exception: If there is an error during the import process.
        """
        try:
            # Call the 'import_csv_data' stored procedure with the provided CSV path
            return self.call_procedure("import_csv_data", csv_path)
        except Exception as e:
            logging.error(f"Error importing CSV data from {csv_path}: {e}")
            raise
    

    def add_deposit_record(self, p_refid, p_chat_id, p_firstname, p_lastname, p_amount, p_asset, p_txid, p_deposit_address):
        """
        Adds a deposit record to the database using a stored procedure.

        Args:
            p_refid (str): Reference ID for the deposit.
            p_chat_id (int): Chat ID associated with the user making the deposit.
            p_firstname (str): First name of the user making the deposit.
            p_lastname (str): Last name of the user making the deposit.
            p_amount (float): Amount of the deposit.
            p_asset (str): Asset type of the deposit (e.g., currency).
            p_txid (str): Transaction ID associated with the deposit.
            p_deposit_address (str): Deposit address used for the transaction.

        Returns:
            list: List of dictionaries containing any results returned by the stored procedure.
                Each dictionary represents a row of results with column names as keys.

        Raises:
            Exception: If there is an error while adding the deposit record.
        """
        try:
            # Call the 'add_deposit_record' stored procedure with the provided parameters
            return self.call_procedure("add_deposit_record", p_refid, p_chat_id, p_firstname, p_lastname, p_amount, p_asset, p_txid, p_deposit_address)
        except Exception as e:
            logging.error(f"Error adding deposit record with refid {p_refid}: {e}")
            raise
    

    def check_if_deposit_processed(self, p_refid):
        """
        Checks if a deposit with the given reference ID has been processed.

        Args:
            p_refid (str): Reference ID of the deposit to check.

        Returns:
            bool or None: True if the deposit has been processed, False if not, None if no result.

        Raises:
            Exception: If there is an error while checking the deposit status.
        """
        try:
            # Call the 'check_if_deposit_processed' function with the provided reference ID
            result = self.call_function('check_if_deposit_processed', p_refid)
            
            # Extract the boolean result from the fetched data
            return result[0]['check_if_deposit_processed'] if result else None
        except Exception as e:
            logging.error(f"Error checking if deposit with refid {p_refid} is processed: {e}")
            raise
    

    def compound_returns(self, p_current_date):
        """
        Executes the 'compound_returns' procedure to compound returns for all balances.

        Args:
            p_current_date (date): Current date for which returns are to be compounded.

        Returns:
            list or None: List of results from the 'compound_returns' procedure, or None if no results.

        Raises:
            Exception: If there is an error while compounding returns.
        """
        try:
            # Call the 'compound_returns' procedure with the provided current date
            return self.call_procedure("compound_returns", p_current_date)
        except Exception as e:
            logging.error(f"Error compounding returns for date {p_current_date}: {e}")
            raise
    

    
    def get_statement(self, p_chat_id, p_start_date, p_end_date):
        """
        Retrieves statement entries for a given chat_id and date range.

        Args:
            p_chat_id (int): Chat ID of the client for whom the statement is requested.
            p_start_date (datetime.date): Start date of the statement period.
            p_end_date (datetime.date): End date of the statement period.

        Returns:
            list or None: List of statement entries fetched from the 'statement' function,
                        or None if no entries are found.

        Raises:
            Exception: If there is an error while fetching the statement entries.
        """
        try:
            # Call the 'statement' function with the provided parameters
            return self.call_function("statement", p_chat_id, p_start_date, p_end_date)
        except Exception as e:
            logging.error(f"Error fetching statement for chat_id {p_chat_id}: {e}")
            raise
        

    def get_total_liabilities(self):
        """
        Retrieves total liabilities information.

        Returns:
            dict or None: Dictionary containing total liabilities information fetched from the 'total_liabilities' procedure,
                        or None if no data is found.

        Raises:
            Exception: If there is an error while fetching total liabilities information.
        """
        try:
            # Call the 'total_liabilities' procedure
            return self.call_procedure("total_liabilities")
        except Exception as e:
            logging.error(f"Error fetching total liabilities: {e}")
            raise
    

    def projected_balance(self, p_target_date, p_chat_id):
        """
        Calculates the projected balance for a given target date and client chat ID.

        Args:
            p_target_date (date): Target date for calculating the projected balance.
            p_chat_id (int): Client chat ID.

        Returns:
            dict or None: Dictionary containing projected balance information fetched from the 'projected_balance' procedure,
                        or None if no data is found.

        Raises:
            Exception: If there is an error while calculating the projected balance.
        """
        try:
            # Call the 'projected_balance' procedure with the provided parameters
            return self.call_procedure("projected_balance", p_target_date, p_chat_id)
        except Exception as e:
            logging.error(f"Error calculating projected balance: {e}")
            raise
    

    def get_depositaddresses(self):
        """
        Retrieves all deposit addresses from the database.

        Returns:
            list of dict or None: List of dictionaries containing deposit addresses fetched from the
                                  'get_depositaddresses' function, or None if no data is found.

        Raises:
            Exception: If there is an error while retrieving deposit addresses.
        """
        try:
            # Call the 'get_depositaddresses' function
            return self.call_function("get_depositaddresses")
        except Exception as e:
            logging.error(f"Error retrieving deposit addresses: {e}")
            raise


    def get_deposit_address_private_key(self, deposit_address):
        """
        Retrieves the unhashed private key for a given deposit address.

        Args:
            deposit_address (str): The deposit address to search for.

        Returns:
            bytes: The unhashed private key corresponding to the deposit address.

        Raises:
            Exception: If there is an error while retrieving the private key.
        """
        try:
            # Call the 'get_unhashed_private_key' function
            private_key = self.call_function("get_deposit_address_private_key", deposit_address)
            return private_key
        except Exception as e:
            logging.error(f"Error retrieving unhashed private key: {e}")
            raise


    def get_centraladdress(self):
        """
        Retrieves the central deposit address from the database.
        The central deposit address is the address where all funds found on the
        deposit addresses are transferred to.

        Returns:
            dict: A dictionary containing the central deposit address and other relevant
                details fetched from the 'get_centraladdress' function.
                If no data is found, returns a default address from CONFIG.ETHPOLYGON.BACKUP_CENTRAL_ADDRESS.

        Raises:
            Exception: If there is an error while retrieving the central address.
        """
        try:
            # Call the 'get_depositaddresses' function
            central_address = self.call_function("get_centraladdress")
            if central_address and len(central_address) > 0:
                return central_address
            else:
                return {"depositaddress": CONFIG.ETHPOLYGON.BACKUP_CENTRAL_ADDRESS}
        except Exception as e:
            logging.error(f"Error retrieving central address: {e}")
            raise


    def insert_depositlogs(self, logs):
        try:
            # Prepare lists from logs
            from_addresses = [log['from_address'] for log in logs]
            to_addresses = [log['to_address'] for log in logs]
            transaction_ids = [log['transaction_id'] for log in logs]
            block_numbers = [log['block_number'] for log in logs]
            
            # Block timestamps are already strings in the correct format
            block_timestamps = [log['block_timestamp'] for log in logs]
            amounts = [log['amount'] for log in logs]
            
            # Convert lists to PostgreSQL array format
            from_addresses_str = '{' + ','.join(f"'{addr}'" for addr in from_addresses) + '}'
            to_addresses_str = '{' + ','.join(f"'{addr}'" for addr in to_addresses) + '}'
            transaction_ids_str = '{' + ','.join(f"'{txid}'" for txid in transaction_ids) + '}'
            block_numbers_str = '{' + ','.join(map(str, block_numbers)) + '}'
            block_timestamps_str = '{' + ','.join(f"'{ts}'" for ts in block_timestamps) + '}'
            amounts_str = '{' + ','.join(map(str, amounts)) + '}'

            # Call the stored procedure with explicit type casting
            self.call_procedure(
                "insert_depositlogs",
                from_addresses_str,
                to_addresses_str,
                transaction_ids_str,
                block_numbers_str,
                block_timestamps_str,
                amounts_str,
            )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error inserting deposit logs: {e}")
            self.conn.rollback()
            raise


    def get_newdepositlogs(self):
        try:
            # Call the 'get_depositaddresses' function
            newdeposits = self.call_function("get_newdepositlogs")
            if newdeposits and len(newdeposits) > 0:
                result = []
                for row in newdeposits:
                    dict_row = dict(row)
                    for key, value in dict_row.items():
                        if isinstance(value, str):
                            dict_row[key] = value.strip("'")
                        else:
                            dict_row[key] = value
                    result.append(dict_row)
                return result
            else:
                return None
        except Exception as e:
            logging.error(f"Error retrieving new deposit logs: {e}")
            raise


    def get_unidentified_deposits(self):
        """
        Retrieves all unidentified depositsfrom the database.

        Returns:
            list of dict containing the depositlog rows

        Raises:
            Exception: If there is an error while retrieving deposit addresses.
        """
        try:
            # create cursor
            cursor = self.conn.cursor()

            # execute sql query
            cursor.execute("SELECT * FROM depositlogs_view ORDER BY block_timestamp DESC LIMIT 300")

            # fetch results
            deposits = cursor.fetchall()

            # convert to list of dictionaries
            columns = [desc[0] for desc in cursor.description]
            deposits_list = [dict(zip(columns, row)) for row in deposits]

            return deposits_list
        except Exception as e:
            logging.error(f"Error retrieving deposit addresses: {e}")
            raise


    def update_depositlogs_refund(self, p_transaction_id, p_refund_transaction_id):
        """
        Retrieves all unidentified depositsfrom the database.

        Returns:
            list of dict containing the depositlog rows

        Raises:
            Exception: If there is an error while retrieving deposit addresses.
        """
        try:
            # Call the 'get_depositaddresses' function
            self.call_function("update_depositlogs_refund", p_transaction_id, p_refund_transaction_id)
        except Exception as e:
            logging.error(f"Error updating depositlog with refund data: {e}")
            raise


    def get_profits_one_day(self, p_chat_id):
        url = f"{CONFIG.RETURNS_BASEURL}/get_profits_one_day"
        print(f"URL: {url}")
        try:
            response = requests.post(url, params={
                'chat_id': p_chat_id
            })
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving one day profits: {e}")
            raise


    def get_profits_one_week(self, p_chat_id):
        url = f"{CONFIG.RETURNS_BASEURL}/get_profits_one_week"
        print(f"URL: {url}")
        try:
            response = requests.post(url, params={
                'chat_id': p_chat_id
            })
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving one week profits: {e}")
            raise

        
    def get_profits_one_month(self, p_chat_id):
        url = f"{CONFIG.RETURNS_BASEURL}/get_profits_one_month"
        print(f"URL: {url}")
        try:
            response = requests.post(url, params={
                'chat_id': p_chat_id
            })
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving one month profits: {e}")
            raise


    def get_profits_three_months(self, p_chat_id):
        url = f"{CONFIG.RETURNS_BASEURL}/get_profits_three_months"
        print(f"URL: {url}")
        try:
            response = requests.post(url, params={
                'chat_id': p_chat_id
            })
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving three months profits: {e}")
            raise


    def get_profits_all_time(self, p_chat_id):
        url = f"{CONFIG.RETURNS_BASEURL}/get_profits_all_time"
        print(f"URL: {url}")
        try:
            response = requests.post(url, params={
                'chat_id': p_chat_id
            })
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving yesterday's return: {e}")
            raise


    def get_bot_returns_yesterday(self):
        url = f"{CONFIG.RETURNS_BASEURL}/get_bot_returns_yesterday"
        print(f"URL: {url}")
        try:
            response = requests.post(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving one day profits: {e}")
            raise


    def calculate_weekly_compounded_return(self):
        url = f"{CONFIG.RETURNS_BASEURL}/calculate_weekly_compounded_return"
        print(f"URL: {url}")
        try:
            response = requests.post(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving one week compounded returns: {e}")
            raise


    def calculate_monthly_compounded_return(self):
        url = f"{CONFIG.RETURNS_BASEURL}/calculate_monthly_compounded_return"
        print(f"URL: {url}")
        try:
            response = requests.post(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving one month compounded returns: {e}")
            raise


    def calculate_three_months_compounded_return(self):
        url = f"{CONFIG.RETURNS_BASEURL}/calculate_three_months_compounded_return"
        print(f"URL: {url}")
        try:
            response = requests.post(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error retrieving three months compounded returns: {e}")
            raise


    def get_total_deposits_client(self, p_chat_id: int):
        url = f"{CONFIG.RETURNS_BASEURL}/get_total_deposits_client"
        print(f"URL: {url}")
        
        try:
            # Make a POST request with the chat_id as a parameter
            response = requests.get(url, params={"chat_id": p_chat_id})
            response.raise_for_status()  # Check if the request was successful
            
            # Parse the response as JSON
            data = response.json()
            
            # Assuming the API returns a JSON object with the total deposit as a float
            client_total_deposit = float(data)  # Ensure it's a float
            
            return client_total_deposit
        
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            raise
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err}")
            raise
        except ValueError as val_err:
            logging.error(f"Value error occurred: {val_err}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise



    def update_transferred_status_true(self, transaction_id):
        """
        Updates the 'transferred' boolean field in depositlogs to TRUE by transaction_id (varchar).
    
        Returns:
            nothing, except error message in case of exception

        Raises:
            Exception: If there is an error while retrieving deposit addresses.
        """
        try:
            # Call the 'get_depositaddresses' function
            return self.call_function("update_transferred_status_true", transaction_id)
        except Exception as e:
            logging.error(f"Error updating depositlogs, 'transferred' to TRUE (tx_hash = {transaction_id}): {e}")
            raise


    def update_transferred_status_false(self, transaction_id):
        """
        Updates the 'transferred' boolean field in depositlogs to FALSE by transaction_id (varchar).
    
        Returns:
            nothing, except error message in case of exception

        Raises:
            Exception: If there is an error while retrieving deposit addresses.
        """
        try:
            # Call the 'get_depositaddresses' function
            return self.call_function("update_transferred_status_false", transaction_id)
        except Exception as e:
            logging.error(f"Error updating depositlogs, 'transferred' to FALSE (tx_hash = {transaction_id}): {e}")
            raise



    def close(self):
        """
        Closes the database connection.

        Logs an info message when the database connection is successfully closed.

        Raises:
            Exception: If there is an error while closing the database connection.
        """
        try:
            if self.conn:
                self.conn.close()
                logging.info("Database connection closed")
        except Exception as e:
            logging.error(f"Error closing database connection: {e}")
            raise


    




"""
This script is designed to import data from a CSV file containing returns into a database using the DataHandler class.

It utilizes argparse to handle command-line arguments, allowing customization of the CSV file name to be imported.
Upon execution, the script initializes a DataHandler instance connected to the database configured in the CONFIG module.
The import_csv_data method of DataHandler is then called with the specified or default CSV file name.

Command-line Arguments:
    --file_name (str): Optional. Specifies the name of the CSV file containing returns data. Defaults to 'returns.csv'.

Execution Flow:
1. Argument Parsing:
   - Parses command-line arguments to retrieve the file name of the CSV containing returns data.

2. Database Connection:
   - Establishes a connection to the database using credentials and configuration from the CONFIG module.

3. Data Import:
   - Invokes the import_csv_data method of the DataHandler instance to import data from the specified CSV file into the database.

Error Handling:
- Exceptions encountered during execution are logged using Python's logging module, providing detailed error messages for debugging purposes.

Note:
- Ensure that the CONFIG module contains accurate database connection details (DBNAME, USER, PASSWORD, HOST, PORT) for successful execution.
- Adjust the default file name ('returns.csv') or specify a different file name using the --file_name argument as needed.

Usage Example:
$ python script_name.py --file_name my_returns_data.csv

"""

if __name__ == "__main__":
    try:
        import argparse

        # Initialize argument parser
        parser = argparse.ArgumentParser(description='Import CSV file containing returns')
        parser.add_argument('--file_name', type=str, default='returns.csv', help='CSV file name (default: returns.csv)')

        # Parse command-line arguments
        args = parser.parse_args()

        # Initialize DataHandler instance
        data = DataHandler()

        # Call import_csv_data method with the provided file name
        data.import_csv_data(args.file_name)

    except Exception as e:
        # Log any errors that occur during execution
        import logging
        logging.error(f"Error in main execution: {e}")
        raise