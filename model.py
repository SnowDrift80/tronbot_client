# model.py

import psycopg2
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
        - withdraw(p_chat_id, p_amount): Processes a withdrawal transaction and updates balances accordingly.
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
    

    def correct_balance(self, p_chat_id, p_amount):
        """
        Executes the 'correct_balance' procedure to adjust the balance for a specific chat_id.

        Args:
            p_chat_id (int): Chat ID of the client whose balance is to be corrected.
            p_amount (float): Amount by which to correct the balance.

        Returns:
            list or None: List of results from the 'correct_balance' procedure, or None if no results.

        Raises:
            Exception: If there is an error while correcting the balance.
        """
        try:
            # Call the 'correct_balance' procedure with the provided chat_id and amount
            return self.call_procedure('correct_balance', p_chat_id, p_amount)
        except Exception as e:
            logging.error(f"Error correcting balance for chat_id {p_chat_id}: {e}")
            raise
    

    def handle_deposit(self, p_chat_id, p_firstname, p_lastname, p_currency, p_method, p_amount, p_deposit_address, p_kraken_refid, p_kraken_time, p_kraken_txid):
        """
        Handles a deposit by calling the 'handle_deposit' procedure.

        Args:
            p_chat_id (int): Chat ID of the client making the deposit.
            p_firstname (str): First name of the client.
            p_lastname (str): Last name of the client.
            p_currency (str): Currency of the deposit.
            p_method (str): Method used for the deposit.
            p_amount (float): Amount deposited.
            p_deposit_address (str): Deposit address used.
            p_kraken_refid (str): Reference ID from Kraken exchange.
            p_kraken_time (datetime): Time of the deposit from Kraken.
            p_kraken_txid (str): Transaction ID from Kraken.

        Returns:
            list or None: List of results from the 'handle_deposit' procedure, or None if no results.

        Raises:
            Exception: If there is an error while handling the deposit.
        """
        try:
            # Call the 'handle_deposit' procedure with the provided parameters
            return self.call_procedure('handle_deposit', p_chat_id, p_firstname, p_lastname, p_currency, p_method,
                                    p_amount, p_deposit_address, p_kraken_refid, p_kraken_time, p_kraken_txid)
        except Exception as e:
            logging.error(f"Error handling deposit for chat_id {p_chat_id}: {e}")
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
    

    def withdraw(self, p_chat_id, p_amount):
        """
        Processes a withdrawal for a client specified by chat ID.

        Args:
            p_chat_id (int): Client chat ID.
            p_amount (float): Amount to withdraw.

        Returns:
            dict or None: Dictionary containing withdrawal details fetched from the 'withdraw' procedure,
                        or None if no data is found.

        Raises:
            Exception: If there is an error while processing the withdrawal.
        """
        try:
            # Call the 'withdraw' procedure with the provided parameters
            return self.call_procedure("withdraw", p_chat_id, p_amount)
        except Exception as e:
            logging.error(f"Error processing withdrawal: {e}")
            raise
    
    
    def get_balance(self, p_chat_id):
        """
        Retrieves the current balance for a client specified by chat ID.

        Args:
            p_chat_id (int): Client chat ID.

        Returns:
            dict or None: Dictionary containing balance details fetched from the 'get_balance' function,
                        or None if no data is found.

        Raises:
            Exception: If there is an error while retrieving the balance.
        """
        try:
            # Call the 'get_balance' function with the provided chat ID
            return self.call_function("get_balance", p_chat_id)
        except Exception as e:
            logging.error(f"Error retrieving balance: {e}")
            raise


    def get_client(self, p_chat_id):
        """
        Retrieves client details based on the provided chat ID.

        Args:
            p_chat_id (int): Client chat ID.

        Returns:
            dict or None: Dictionary containing client details fetched from the 'get_client' function,
                        or None if no data is found.

        Raises:
            Exception: If there is an error while retrieving client details.
        """
        try:
            # Call the 'get_client' function with the provided chat ID
            return self.call_function("get_client", p_chat_id)
        except Exception as e:
            logging.error(f"Error retrieving client details: {e}")
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