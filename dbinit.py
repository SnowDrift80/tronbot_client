"""
Add extension pgcrpto extension by running the blow line in PGAdmin4 Query Tool
CREATE EXTENSION IF NOT EXISTS pgcrypto;
"""


import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from config import CONFIG


class DBInit:
    def __init__(self) -> None:
        self.db_config = CONFIG.DBCONFIG

    
    def connect(self):
        """Establish connection to postgres db"""
        return psycopg2.connect(
            dbname=self.db_config.DBNAME,
            user=self.db_config.USER,
            password=self.db_config.PASSWORD,
            host=self.db_config.HOST,
            port=self.db_config.PORT
        )
    

    def execute_script(self, script):
        """execute a single SQL script"""
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(script)
                conn.commit()
    

    def initialize_database(self):
        """run all the sql scripts to initialize db"""
        self.execute_script(self.create_deposits_table())
        self.execute_script(self.create_depositaddresses_table())
        self.execute_script(self.create_import_depositaddresses_table())
        self.execute_script(self.create_replace_depositaddresses_procedure())
        self.execute_script(self.create_get_depositaddresses_function())
        self.execute_script(self.create_get_deposit_address_private_key())
        self.execute_script(self.create_update_transferred_status_true())
        self.execute_script(self.create_update_transferred_status_false())
        self.execute_script(self.create_get_centraladdress_function())
        self.execute_script(self.create_depositlogs_table())
        self.execute_script(self.create_depositlogs_view())
        self.execute_script(self.create_update_depositlogs_refund_function())
        self.execute_script(self.create_get_newdepositlogs())
        self.execute_script(self.create_insert_depositlogs_procedure())
        self.execute_script(self.create_import_csv_data())
        self.execute_script(self.create_add_deposit_record())
        self.execute_script(self.create_check_if_deposit_processed())



    @staticmethod
    def create_depositlogs_view():
        """Returns SQL query string to create 'depositlogs_view' view if it doesn't exist.

        This static method generates an SQL query string that creates the 'depositlogs_view' view
        if it does not already exist in the database. The view includes fields from the depositlogs
        table where the 'transferred' field is true and the 'transaction_id' is not present in the
        'refid' column of the deposits table.

        Returns:
            str: SQL query string to create the 'depositlogs_view' view with the specified conditions.
        """
        return """
        CREATE OR REPLACE VIEW depositlogs_view AS
        SELECT dl.transaction_id,
            dl.from_address,
            dl.to_address,
            dl.block_number,
            dl.block_timestamp,
            dl.amount,
            dl.transferred,
            dl.refund_transaction_id,
            dl.refund_timestamp
        FROM depositlogs dl
        WHERE dl.transferred = true
        AND REPLACE(dl.transaction_id, '''', '') NOT IN (
                SELECT deposits.refid
                    FROM deposits
            );
        """
   

    @staticmethod
    def create_deposits_table():
        """Returns SQL query string to create 'deposits' table if it doesn't exist.

        This static method generates an SQL query string that creates the 'deposits' table
        if it does not already exist in the database. The table includes columns for reference ID,
        chat ID, first name, last name, amount, asset type, transaction ID (txid), deposit address,
        and timestamp of the deposit.

        Returns:
            str: SQL query string to create the 'deposits' table with appropriate schema
                 and an index on 'refid' for faster lookup.
        """
        return """
        CREATE TABLE IF NOT EXISTS deposits (
            refid VARCHAR(100) PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            amount NUMERIC(20, 6) NOT NULL,
            asset VARCHAR(50),
            txid VARCHAR(100),
            deposit_address VARCHAR(255),
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Index on refid for faster lookup
        CREATE INDEX IF NOT EXISTS idx_deposits_refid ON deposits(refid);
        """
    

    @staticmethod
    def create_depositaddresses_table():
        """Returns SQL query string to create 'depositaddresses' table if it doesn't exist.

        This static method generates an SQL query string that creates the 'depositaddresses' table
        if it does not already exist in the database. The table includes columns for a unique reference ID,
        deposit address, chat ID to which the address is assigned (which can be NULL), the expiration date of the assignment,
        and balances for MATIC and USDT.

        Returns:
            str: SQL query string to create the 'depositaddresses' table with appropriate schema
                 and an index on 'assigned_to_chatid' for faster lookup.
        """
        return """
        CREATE TABLE IF NOT EXISTS depositaddresses (
            refid SERIAL PRIMARY KEY,
            account_type VARCHAR(1) NOT NULL, -- D for Deposit, C for Central Account, H for Helper Account
            account_name VARCHAR(100) NOT NULL,  -- account name
            depositaddress VARCHAR(42) NOT NULL, -- ethereum address
            private_key BYTEA NOT NULL,    -- secret private key
            assigned_to_chatid BIGINT,  -- This column can be NULL
            assignment_expiration_datetime TIMESTAMP,
            MATIC_balance NUMERIC(20, 6) DEFAULT 0,
            USDT_balance NUMERIC(20, 6) DEFAULT 0
        );

        -- Index on assigned_to_chatid for faster lookup, but allowing NULL values
        CREATE INDEX IF NOT EXISTS idx_depositaddresses_chatid ON depositaddresses(assigned_to_chatid);
        """    
    

    @staticmethod
    def create_replace_depositaddresses_procedure():
        """Returns SQL query string to create 'replace_depositaddresses' stored procedure.

        This static method generates an SQL query string to create a stored procedure named 'replace_depositaddresses'.
        The procedure performs the following operations:
        1. Deletes all existing records from the 'depositaddresses' table.
        2. Inserts all records from the 'import_depositaddresses' table into 'depositaddresses'.
        3. Hashes the 'private_key' field during the insertion into 'depositaddresses'.

        Returns:
            str: SQL query string to create the 'replace_depositaddresses' stored procedure.
        """
        return """
        CREATE OR REPLACE PROCEDURE replace_depositaddresses()
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- Delete all existing records from depositaddresses
            DELETE FROM depositaddresses;

            -- Insert all records from import_depositaddresses into depositaddresses
            INSERT INTO depositaddresses (account_type, account_name, depositaddress, private_key, assigned_to_chatid, assignment_expiration_datetime, MATIC_balance, USDT_balance)
            SELECT
                account_type,
                account_name,
                depositaddress,
                pgp_sym_encrypt(private_key, 'dh/afHrC2mO6xPyrhtizM1J1zM7CSMaRUOkj4am7XSU=') AS hashed_private_key,  -- Hashing the private_key
                assigned_to_chatid,
                assignment_expiration_datetime,
                MATIC_balance,
                USDT_balance
            FROM
                import_depositaddresses;

            -- Delete the temporary data in table import_depositaddresses
            DELETE FROM import_depositaddresses;
        END;
        $$;
        """
    
    @staticmethod
    def create_get_deposit_address_private_key():
        return """
        CREATE OR REPLACE FUNCTION get_deposit_address_private_key(deposit_address VARCHAR)
        RETURNS TEXT AS $$
        DECLARE
            encrypted_private_key BYTEA;
            decrypted_private_key TEXT;  -- Change to TEXT to return as a string
        BEGIN
            -- Retrieve the encrypted private key for the given deposit address
            SELECT private_key INTO encrypted_private_key
            FROM depositaddresses
            WHERE depositaddress = deposit_address;

            -- Check if the private key was found
            IF encrypted_private_key IS NULL THEN
                RAISE EXCEPTION 'No record found for deposit address: %', deposit_address;
            END IF;

            -- Decrypt the private key
            decrypted_private_key := pgp_sym_decrypt(encrypted_private_key, 'dh/afHrC2mO6xPyrhtizM1J1zM7CSMaRUOkj4am7XSU=')::TEXT;  -- Cast to TEXT

            RETURN decrypted_private_key;  -- Return as TEXT
        END;
        $$ LANGUAGE plpgsql;
        """
            

    @staticmethod
    def create_import_depositaddresses_table():
        """Returns SQL query string to create 'import_depositaddresses' table if it doesn't exist.

        This static method generates an SQL query string that creates the 'import_depositaddresses' table
        if it does not already exist in the database. The table includes columns for a unique reference ID,
        account name, deposit address, private key, chat ID to which the address is assigned (which can be NULL),
        the expiration date of the assignment, and balances for MATIC and USDT.

        Returns:
            str: SQL query string to create the 'import_depositaddresses' table with appropriate schema
                 and an index on 'assigned_to_chatid' for faster lookup.
        """
        return """
        CREATE TABLE IF NOT EXISTS import_depositaddresses (
            refid SERIAL PRIMARY KEY,
            account_type VARCHAR(1) NOT NULL, -- D for Deposit, C for Central Account, H for Helper Account
            account_name VARCHAR(100) NOT NULL,  -- account name
            depositaddress VARCHAR(42) NOT NULL, -- ethereum address
            private_key VARCHAR(64) NOT NULL,    -- secret private key
            assigned_to_chatid BIGINT,  -- This column can be NULL
            assignment_expiration_datetime TIMESTAMP,
            MATIC_balance NUMERIC(20, 6) DEFAULT 0,
            USDT_balance NUMERIC(20, 6) DEFAULT 0
        );

        -- Index on assigned_to_chatid for faster lookup, but allowing NULL values
        CREATE INDEX IF NOT EXISTS idx_import_depositaddresses_chatid ON import_depositaddresses(assigned_to_chatid);
        """    
    

    @staticmethod
    def create_get_depositaddresses_function():
        """Creates a stored function that returns all rows from the 'depositaddresses' table."""
        return """
        CREATE OR REPLACE FUNCTION get_depositaddresses()
        RETURNS TABLE (
            refid INT,
            account_type VARCHAR(1),
            account_name VARCHAR(100),
            depositaddress VARCHAR(42),
            assigned_to_chatid BIGINT,
            assignment_expiration_datetime TIMESTAMP,
            MATIC_balance NUMERIC(20, 6),
            USDT_balance NUMERIC(20, 6)
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                d.refid,
                d.account_type,
                d.account_name,
                d.depositaddress,
                d.assigned_to_chatid,
                d.assignment_expiration_datetime,
                d.MATIC_balance,
                d.USDT_balance
            FROM 
                depositaddresses d
            WHERE d.account_type = 'D';
        END;
        $$ LANGUAGE plpgsql;
        """


    @staticmethod
    def create_get_centraladdress_function():
        """Creates a stored function that returns the single central address (address_type "C")."""
        return """
        CREATE OR REPLACE FUNCTION get_centraladdress()
        RETURNS TABLE (
            refid INT,
            account_type VARCHAR(1),
            account_name VARCHAR(100),
            depositaddress VARCHAR(42),
            assigned_to_chatid BIGINT,
            assignment_expiration_datetime TIMESTAMP,
            MATIC_balance NUMERIC(20, 6),
            USDT_balance NUMERIC(20, 6)
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                d.refid,
                d.account_type,
                d.account_name,
                d.depositaddress,
                d.assigned_to_chatid,
                d.assignment_expiration_datetime,
                d.MATIC_balance,
                d.USDT_balance
            FROM 
                depositaddresses d
            WHERE d.account_type = 'C'
            LIMIT 1;  -- Ensure only one row is returned
        END;
        $$ LANGUAGE plpgsql;
        """

    
    @staticmethod
    def create_depositlogs_table():
        """
        Creates the 'depositlogs' table to log all transactions involving the designated deposit addresses.
        This table will store details about each transaction, including the addresses involved, 
        transaction ID, block information, and transfer amount. Additionally, it includes a 
        'chat_id' field that will be updated later to associate the transaction with a specific 
        Telegram chat.

        Returns:
            str: SQL string to create the 'depositlogs' table.
        """        
        return """
        CREATE TABLE IF NOT EXISTS depositlogs(
            id SERIAL PRIMARY KEY, -- unique identifier for each row
            from_address VARCHAR(100) NOT NULL, -- Address from which the transfer was made
            to_address VARCHAR(100) NOT NULL, -- Address to which the transfer was made
            transaction_id VARCHAR(100) NOT NULL, -- Unique transaction id (ETH "Transaction hash")
            block_number BIGINT NOT NULL, -- block number in blockchain in which the transaction happened.
            block_timestamp TIMESTAMP NOT NULL, -- Timestamp of the block
            amount NUMERIC(20, 6) NOT NULL, -- Amount of USDT transferred
            chat_id BIGINT, -- Telegram chat ID
            transferred BOOLEAN DEFAULT FALSE, -- Boolean field to indicate whether the transfer was processed
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- automatically set to the current timestamp.
            refund_transaction_id VARCHAR(100), -- Optional trx ID from refund if unidentified deposit returned to sender
            refund_timestamp TIMESTAMP -- Optional timestamp in case of deposit refund 
        );
        -- create an index on 'transaction_id'
        CREATE INDEX IF NOT EXISTS idx_transaction_id ON depositlogs(transaction_id);

        -- create an index on 'chat_id' (the telegram user ID)
        CREATE INDEX IF NOT EXISTS idx_chat_id ON depositlogs(chat_id);

        -- Create an index on 'transferred' to optimize queries filtering by this field
        CREATE INDEX IF NOT EXISTS idx_transferred ON depositlogs(transferred);
        """


    @staticmethod
    def create_update_depositlogs_refund_function():
        """
        Creates the stored function 'update_depositlogs_returndata' which updates the 'refund_transaction_id'
        and 'refund_timestamp' fields in the 'depositlogs' table for a specific record identified by its 'id'.

        Returns:
            str: SQL string to create the 'update_depositlogs_returndata' function.
        """
        return """
        CREATE OR REPLACE FUNCTION update_depositlogs_refund(
            p_transaction_id VARCHAR(100),
            p_refund_transaction_id VARCHAR(100)
        )
        RETURNS VOID AS $$
        BEGIN
            -- Update the depositlogs table with the refund transaction id and current timestamp
            UPDATE depositlogs
            SET
                refund_transaction_id = p_refund_transaction_id,
                refund_timestamp = CURRENT_TIMESTAMP
            WHERE
                transaction_id = p_transaction_id;

            -- Optionally, you can raise a notice or log the action
            RAISE NOTICE 'Deposit log with Transaction ID % updated with refund transaction ID % and current timestamp.', p_transaction_id, p_refund_transaction_id;
        END;
        $$ LANGUAGE plpgsql;
        """    


    @staticmethod
    def create_get_newdepositlogs():
        """
        Returns all depositlogs where 'transferred' is false. This means, these are the transactions
        from clients depositing money on one of the deposit-accounts which haven't been transferred yet
        to the central account.
        """
        return """
        CREATE OR REPLACE FUNCTION get_newdepositlogs()
        RETURNS TABLE (
            id INTEGER,  -- Updated type to match the table definition
            from_address VARCHAR,
            to_address VARCHAR,
            transaction_id VARCHAR,
            block_number BIGINT,
            block_timestamp TIMESTAMP,
            amount NUMERIC,  -- Adjusted type to match table definition if needed
            chat_id BIGINT,
            transferred BOOLEAN,
            created_at TIMESTAMP
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                d.id,
                d.from_address,
                d.to_address,
                d.transaction_id,
                d.block_number,
                d.block_timestamp,
                d.amount,
                d.chat_id,
                d.transferred,
                d.created_at
            FROM depositlogs d
            WHERE d.transferred = FALSE;
        END;
        $$ LANGUAGE plpgsql;
        """


    @staticmethod
    def create_update_transferred_status_true():
        return """
        CREATE OR REPLACE FUNCTION update_transferred_status_true(param_transaction_id VARCHAR)
        RETURNS VOID AS $$
        BEGIN
            -- Perform the update using the parameter
            UPDATE depositlogs
            SET transferred = TRUE
            WHERE transaction_id = param_transaction_id;

            -- Check if the update affected any rows
            IF NOT FOUND THEN
                RAISE NOTICE 'No rows updated for transaction_id: %', param_transaction_id;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
        """

    @staticmethod
    def create_update_transferred_status_false():
        return """
        CREATE OR REPLACE FUNCTION update_transferred_status_false(param_transaction_id VARCHAR)
        RETURNS VOID AS $$
        BEGIN
            -- Perform the update using the parameter
            UPDATE depositlogs
            SET transferred = FALSE
            WHERE transaction_id = param_transaction_id;

            -- Check if the update affected any rows
            IF NOT FOUND THEN
                RAISE NOTICE 'No rows updated for transaction_id: %', param_transaction_id;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
        """




    @staticmethod
    def create_insert_depositlogs_procedure():
        """
        Creates a stored procedure to insert new records into the 'depositlogs' table. 
        This procedure accepts arrays of transaction details and inserts records for 
        transactions that are not already present in the table based on the 'transaction_id'. 
        This ensures that only unique transactions are logged. The procedure handles multiple 
        transactions in a single call, using arrays to efficiently manage batch inserts.

        Parameters:
            p_from_address (TEXT[]): Array of addresses from which the transfer was made.
            p_to_address (TEXT[]): Array of addresses to which the transfer was made.
            p_transaction_id (TEXT[]): Array of unique transaction IDs (ETH "Transaction hash").
            p_block_number (BIGINT[]): Array of block numbers in the blockchain where the transaction occurred.
            p_block_timestamp (TIMESTAMP[]): Array of timestamps for the blocks.
            p_amount (NUMERIC[]): Array of amounts of USDT transferred.

        Returns:
            str: SQL string to create the 'insert_depositlogs' procedure.
        """
        return """
        CREATE OR REPLACE PROCEDURE insert_depositlogs(
            p_from_address TEXT[],
            p_to_address TEXT[],
            p_transaction_id TEXT[],
            p_block_number BIGINT[],
            p_block_timestamp TIMESTAMP[],
            p_amount NUMERIC[]
        )
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- Insert new records directly into the depositlogs table
            INSERT INTO depositlogs (
                from_address,
                to_address,
                transaction_id,
                block_number,
                block_timestamp,
                amount
            )
            SELECT
                from_address,
                to_address,
                transaction_id,
                block_number,
                block_timestamp,
                amount
            FROM (
                SELECT
                    unnest(p_from_address) AS from_address,
                    unnest(p_to_address) AS to_address,
                    unnest(p_transaction_id) AS transaction_id,
                    unnest(p_block_number) AS block_number,
                    unnest(p_block_timestamp) AS block_timestamp,
                    unnest(p_amount) AS amount
            ) AS new_records
            WHERE
                new_records.transaction_id NOT IN (
                    SELECT transaction_id
                    FROM depositlogs
                );
        END;
        $$;
        """



    @staticmethod
    def create_import_csv_data():
        """Returns SQL query string to create a stored procedure for importing CSV data.

        This static method generates an SQL query string that defines a stored procedure
        `import_csv_data` in PL/pgSQL language. The procedure executes a COPY command to 
        import data from a CSV file located at the specified path (`csv_path`) into the 
        'returns' table. It expects the CSV file to have headers.

        Args:
            csv_path (str): Path to the CSV file to be imported.

        Returns:
            str: SQL query string to create the stored procedure.
        """
        return """
        CREATE OR REPLACE PROCEDURE import_csv_data(csv_path TEXT)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            EXECUTE FORMAT('COPY returns (date, returns) FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER ",")', csv_path);

            RAISE NOTICE 'CSV data imported successfully from %', csv_path;
        END;
        $$;
        """

        # how to use stored procedure: CALL import_csv_data('/path/data.csv')


    @staticmethod
    def create_add_deposit_record():
        """Returns SQL query string to create a stored procedure for adding a deposit record.

        This static method generates an SQL query string that defines a stored procedure
        `add_deposit_record` in PL/pgSQL language. The procedure inserts a new deposit record
        into the 'deposits' table with provided parameters.

        Args:
            p_refid (str): Unique identifier of the deposit transaction.
            p_chat_id (int): Telegram chat ID of the client.
            p_firstname (str): First name of the client.
            p_lastname (str): Last name of the client.
            p_amount (float): Amount deposited.
            p_asset (str): Asset type deposited.
            p_txid (str): Transaction ID of the deposit.
            p_deposit_address (str): Deposit address used for the deposit.

        Returns:
            str: SQL query string to create the stored procedure.
        """        
        return """
        CREATE OR REPLACE PROCEDURE add_deposit_record(
            p_refid VARCHAR(100),
            p_chat_id BIGINT,
            p_firstname VARCHAR(100),
            p_lastname VARCHAR(100),
            p_amount NUMERIC(20, 6),
            p_asset VARCHAR(50),
            p_txid VARCHAR(100),
            p_deposit_address VARCHAR(100)
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345;
        BEGIN
            -- acquire advisory lock
            PERFORM pg_advisory_lock(lock_id);

            -- insert the new record into table
            INSERT INTO deposits (refid, chat_id, firstname, lastname, amount, asset, txid, deposit_address, time)
            VALUES (p_refid, p_chat_id, p_firstname, p_lastname, p_amount, p_asset, p_txid, p_deposit_address, CURRENT_TIMESTAMP);

            -- release advisory lock
            PERFORM pg_advisory_unlock(lock_id);
        END;
        $$;
        """


    @staticmethod
    def create_check_if_deposit_processed():
        """Returns SQL query string to create a stored function that checks if a deposit was already processed.

        This static method generates an SQL query string that defines a stored function
        `check_if_deposit_processed` in PL/pgSQL language. The function checks if a deposit
        with a given reference ID (`p_refid`) exists in the 'deposits' table.

        Args:
            p_refid (str): Unique identifier of the deposit transaction to check.

        Returns:
            str: SQL query string to create the stored function.
        """
        return """
        CREATE OR REPLACE FUNCTION check_if_deposit_processed(p_refid VARCHAR)
        RETURNS BOOLEAN AS $$
        DECLARE
            v_count INT;
        BEGIN
            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(12345); -- Use your specific lock code instead of 12345
            
            -- Count rows matching the refid
            SELECT COUNT(*) INTO v_count
            FROM deposits
            WHERE refid = p_refid;
            
            -- Release the advisory lock
            PERFORM pg_advisory_unlock(12345); -- Use the same lock code
            
            -- Return true if count is greater than 0, otherwise false
            RETURN v_count > 0;
        END;
        $$ LANGUAGE plpgsql;
        """




# this script must be excuted to initialize database.
if __name__ == "__main__":
    # Instantiate a DBInit object for initializing the database
    db_initializer = DBInit()

    # Call the initialize_database method to perform database initialization
    db_initializer.initialize_database()