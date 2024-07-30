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
        self.execute_script(self.create_clients_table())
        self.execute_script(self.create_deposits_table())
        self.execute_script(self.create_depositaddresses_table())
        self.execute_script(self.create_import_depositaddresses_table())
        self.execute_script(self.create_replace_depositaddresses_procedure())
        self.execute_script(self.create_get_depositaddresses_function())
        self.execute_script(self.create_get_deposit_address_private_key())
        self.execute_script(self.create_update_transferred_status_true())
        self.execute_script(self.create_update_transferred_status_false())
        self.execute_script(self.create_get_centraladdress_function())
        self.execute_script(self.create_balances_table())
        self.execute_script(self.create_ledger_table())
        self.execute_script(self.create_returns_table())
        self.execute_script(self.create_depositlogs_table())
        self.execute_script(self.create_get_newdepositlogs())
        self.execute_script(self.create_insert_depositlogs_procedure())
        self.execute_script(self.create_update_timestamp_trigger())
        self.execute_script(self.create_handle_deposit_procedure())
        self.execute_script(self.create_import_csv_data())
        self.execute_script(self.create_add_deposit_record())
        self.execute_script(self.create_check_if_deposit_processed())
        self.execute_script(self.create_get_client_function())
        self.execute_script(self.create_compound_returns_procedure())
        self.execute_script(self.create_withdraw_procedure())
        self.execute_script(self.create_get_balance_function())
        self.execute_script(self.create_correct_balance_procedure())
        self.execute_script(self.create_statement_function())
        self.execute_script(self.create_total_liabilities_procedure())
        self.execute_script(self.create_projected_balance_procedure())


    @staticmethod
    def create_clients_table():
        """Returns SQL query string to create 'clients' table if it doesn't exist.

        This static method generates an SQL query string that creates the 'clients' table
        if it does not already exist in the database. The table includes columns for primary key,
        chat ID, first name, last name, creation date, and last update date.

        Returns:
            str: SQL query string to create the 'clients' table with appropriate schema.
        """
        return """
        CREATE TABLE IF NOT EXISTS clients(
            primary_key SERIAL PRIMARY KEY,
            chat_id BIGINT UNIQUE NOT NULL,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    def create_balances_table():
        """Returns SQL query string to create 'balances' table if it doesn't exist.

        This static method generates an SQL query string that creates the 'balances' table
        if it does not already exist in the database. The table includes columns for primary key,
        client chat ID (foreign key reference to 'clients' table), currency type, balance amount,
        creation date, and last update date.

        Returns:
            str: SQL query string to create the 'balances' table with appropriate schema.
        """
        return """
        CREATE TABLE IF NOT EXISTS balances (
            primary_key SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL REFERENCES clients(chat_id),
            currency VARCHAR(10) NOT NULL,
            balance NUMERIC(20,6) DEFAULT 0,
            creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    

    @staticmethod
    def create_ledger_table():
        """Returns SQL query string to create 'ledger' table if it doesn't exist.

        This static method generates an SQL query string that creates the 'ledger' table
        if it does not already exist in the database. The table includes columns for primary key,
        transaction type, client chat ID, client first name, client last name, currency type,
        method, transaction amount, deposit address, Kraken reference ID, Kraken transaction time,
        Kraken transaction ID, target address, percentage, conclusion date, and timestamp.

        Returns:
            str: SQL query string to create the 'ledger' table with appropriate schema.
        """
        return """
        CREATE TABLE IF NOT EXISTS ledger (
            primary_key SERIAL PRIMARY KEY,
            transaction_type VARCHAR(20) NOT NULL,
            chat_id BIGINT NOT NULL,
            firstname VARCHAR(100) NOT NULL,
            lastname VARCHAR(100) NOT NULL,
            currency VARCHAR(10),
            method VARCHAR(100),
            amount NUMERIC(20,6),
            deposit_address VARCHAR(255),
            kraken_refid VARCHAR(100),
            kraken_time TIMESTAMP,
            kraken_txid VARCHAR(100),
            target_address VARCHAR(255),
            percentage NUMERIC(10,2),
            conclusion_date TIMESTAMP,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
        """
    

    @staticmethod
    def create_returns_table():
        """Returns SQL query string to create 'returns' table if it doesn't exist.

        This static method generates an SQL query string that creates the 'returns' table
        if it does not already exist in the database. The table includes columns for date and returns.

        Returns:
            str: SQL query string to create the 'returns' table with appropriate schema.
        """
        return """
        CREATE TABLE IF NOT EXISTS returns (
            date DATE PRIMARY KEY,
            returns NUMERIC(12, 6) not null
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_returns_date ON returns (date);
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- automatically set to the current timestamp.
        );
        -- create an index on 'transaction_id'
        CREATE INDEX IF NOT EXISTS idx_transaction_id ON depositlogs(transaction_id);

        -- create an index on 'chat_id' (the telegram user ID)
        CREATE INDEX IF NOT EXISTS idx_chat_id ON depositlogs(chat_id);

        -- Create an index on 'transferred' to optimize queries filtering by this field
        CREATE INDEX IF NOT EXISTS idx_transferred ON depositlogs(transferred);
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
    def create_update_timestamp_trigger():
        """Returns SQL query string to create triggers for updating last_update_date fields.

        This static method generates an SQL query string that creates a trigger function
        and attaches it to specified tables ('clients' and 'balances'). The trigger function
        automatically updates the 'last_update_date' field to the current timestamp whenever
        a row in the respective table is updated.

        Returns:
            str: SQL query string to create trigger functions and attach them to tables.
        """
        return """
        -- Create or replace the function (handles existence by itself)
        CREATE OR REPLACE FUNCTION update_last_update_date()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.last_update_date = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        -- Create the trigger for the clients table if it does not already exist
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'clients_last_update'
            ) THEN
                EXECUTE '
                    CREATE TRIGGER clients_last_update
                    BEFORE UPDATE ON clients
                    FOR EACH ROW
                    EXECUTE FUNCTION update_last_update_date();
                ';
            END IF;
        END $$;

        -- Create the trigger for the balances table if it does not already exist
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'balances_last_update'
            ) THEN
                EXECUTE '
                    CREATE TRIGGER balances_last_update
                    BEFORE UPDATE ON balances
                    FOR EACH ROW
                    EXECUTE FUNCTION update_last_update_date();
                ';
            END IF;
        END $$;
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


    @staticmethod
    def create_handle_deposit_procedure():
        """Returns SQL query string to create a stored procedure to handle deposits.

        This static method generates an SQL query string that defines a stored procedure
        `handle_deposit` in PL/pgSQL language. The procedure handles the deposit process
        by performing the following operations:

        1. Acquires an advisory lock (`pg_advisory_lock`) to ensure mutual exclusion.
        2. Checks if the client exists in the 'clients' table based on `p_chat_id`. If not,
           creates a new client record.
        3. Checks if a balance record exists for the client (`p_chat_id`) and currency
           (`p_currency`). If not, inserts a new balance record; otherwise, updates the
           existing balance.
        4. Inserts a record into the 'ledger' table to log the deposit transaction.
        5. Releases the advisory lock (`pg_advisory_unlock`) after completing the operations.

        Args:
            p_chat_id (int): Chat ID of the client.
            p_firstname (str): First name of the client.
            p_lastname (str): Last name of the client.
            p_currency (str): Currency of the deposit.
            p_method (str): Method used for the deposit.
            p_amount (float): Amount deposited.
            p_deposit_address (str): Deposit address used.
            p_kraken_refid (str): Kraken reference ID of the deposit transaction.
            p_kraken_time (datetime): Timestamp of the deposit transaction in Kraken.
            p_kraken_txid (str): Kraken transaction ID of the deposit.

        Returns:
            str: SQL query string to create the stored procedure.
        """
        return """
        CREATE OR REPLACE PROCEDURE handle_deposit(
            p_chat_id BIGINT,
            p_firstname VARCHAR,
            p_lastname VARCHAR,
            p_currency VARCHAR(10),
            p_method VARCHAR(100),
            p_amount NUMERIC(20, 6),
            p_deposit_address VARCHAR(255),
            p_kraken_refid VARCHAR(100),
            p_kraken_time TIMESTAMP,
            p_kraken_txid VARCHAR(100)
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345; -- Shared lock ID for mutual exclusion
        BEGIN
            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(lock_id);

            BEGIN
                -- check if client exists, if not, create one
                IF NOT EXISTS (SELECT 1 FROM clients WHERE chat_id = p_chat_id) THEN
                    INSERT INTO clients (chat_id, firstname, lastname)
                    VALUES (p_chat_id, p_firstname, p_lastname);
                END IF;
                    
                -- check if balance record exists, if not insert, if yes update
                IF NOT EXISTS (SELECT 1 FROM balances WHERE chat_id = p_chat_id AND currency = p_currency) THEN
                    INSERT INTO balances (chat_id, currency, balance)
                    VALUES (p_chat_id, p_currency, p_amount);
                ELSE
                    UPDATE balances
                    SET balance = balance + p_amount, last_update_date = CURRENT_TIMESTAMP
                    WHERE chat_id = p_chat_id AND currency = p_currency;
                END IF;
                    
                -- insert into ledger
                INSERT INTO ledger (
                    transaction_type,
                    chat_id,
                    firstname, 
                    lastname,
                    currency,
                    method,
                    amount,
                    deposit_address,
                    kraken_refid,
                    kraken_time,
                    kraken_txid,
                    timestamp
                )
                VALUES (
                    'Deposit',
                    p_chat_id,
                    p_firstname,
                    p_lastname,
                    p_currency,
                    p_method,
                    p_amount,
                    p_deposit_address,
                    p_kraken_refid,
                    p_kraken_time,
                    p_kraken_txid,
                    CURRENT_TIMESTAMP
                );
            EXCEPTION WHEN OTHERS THEN
                -- Release the advisory lock in case of error
                PERFORM pg_advisory_unlock(lock_id);
                RAISE;
            END;

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(lock_id);
        END;
        $$;
        """    
 

    @staticmethod
    def create_get_client_function():
        """Returns SQL query string to create a function to retrieve client information by chat ID.

        This static method generates an SQL query string that defines a function `get_client` in PL/pgSQL
        language. The function retrieves client information from the 'clients' table based on the provided
        chat ID (`p_chat_id`).

        Args:
            p_chat_id (int): Chat ID of the client whose information is to be retrieved.

        Returns:
            str: SQL query string to create the function `get_client`.
        """
        return """
        CREATE OR REPLACE FUNCTION get_client(p_chat_id BIGINT)
        RETURNS TABLE (
            chat_id BIGINT,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            creation_date TIMESTAMP,
            last_update_date TIMESTAMP
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            v_chat_id BIGINT;
            v_firstname VARCHAR(100);
            v_lastname VARCHAR(100);
            v_creation_date TIMESTAMP;
            v_last_update_date TIMESTAMP;
        BEGIN
            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(122345);

            -- Fetch the client record
            SELECT
                c.chat_id, -- Prefix with table alias or some unique identifier
                c.firstname,
                c.lastname,
                c.creation_date,
                c.last_update_date
            INTO
                v_chat_id,
                v_firstname,
                v_lastname,
                v_creation_date,
                v_last_update_date
            FROM
                clients c
            WHERE
                c.chat_id = p_chat_id;

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(122345);

            -- Return the fetched record
            RETURN QUERY SELECT v_chat_id, v_firstname, v_lastname, v_creation_date, v_last_update_date;

            -- End the function
            RETURN;
        END;
        $$;
        """


    @staticmethod
    def create_compound_returns_procedure():
        """Returns SQL query string to create a stored procedure for compounding returns and updating balances.

        This static method generates an SQL query string that defines a stored procedure `compound_returns` in PL/pgSQL
        language. The procedure calculates returns for a given date (`p_current_date`) and compounds these returns to update
        balances in the 'balances' table. It also logs these transactions in the 'ledger' table.

        Args:
            p_current_date (DATE): Date for which returns are to be compounded.

        Returns:
            str: SQL query string to create the stored procedure `compound_returns`.
        """
        return """
        CREATE OR REPLACE PROCEDURE compound_returns(p_current_date DATE)
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345; -- Shared lock ID for mutual exclusion
            v_return NUMERIC(12, 6);
            v_increase NUMERIC(20, 6);
            record record;  -- Define a record type variable for the loop
        BEGIN
            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(lock_id);

            BEGIN
                -- Fetch the return value for the given date
                SELECT returns INTO v_return FROM returns WHERE date = p_current_date;

                -- Update all balances
                FOR record IN SELECT * FROM balances
                LOOP
                    v_increase := record.balance * v_return;

                    -- Update the balance
                    UPDATE balances
                    SET balance = balance + v_increase, last_update_date = CURRENT_TIMESTAMP
                    WHERE primary_key = record.primary_key;

                    -- Insert the ledger entry
                    INSERT INTO ledger (
                        transaction_type,
                        chat_id,
                        firstname,
                        lastname,
                        currency,
                        method,
                        amount,
                        percentage,
                        timestamp
                    )
                    VALUES (
                        'Returns',
                        record.chat_id,
                        (SELECT firstname FROM clients WHERE chat_id = record.chat_id),
                        (SELECT lastname FROM clients WHERE chat_id = record.chat_id),
                        'USDT',
                        'Tether USDT (ERC20)',
                        v_increase,
                        v_return,
                        CURRENT_TIMESTAMP
                    );
                END LOOP;
            EXCEPTION WHEN OTHERS THEN
                -- Release the advisory lock in case of error
                PERFORM pg_advisory_unlock(lock_id);
                RAISE;
            END;

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(lock_id);
        END;
        $$;
        """


    @staticmethod
    def create_withdraw_procedure():
        """Returns SQL query string to create a stored procedure for handling withdrawals and updating balances.

        This static method generates an SQL query string that defines a stored procedure `withdraw` in PL/pgSQL language.
        The procedure handles withdrawals for a given `p_chat_id` and `p_amount`. It fetches the current balance of the client,
        calculates the new balance after withdrawal, updates the 'balances' table, and logs the transaction in the 'ledger' table.

        Args:
            p_chat_id (BIGINT): Chat ID of the client making the withdrawal.
            p_amount (NUMERIC(20, 6)): Amount to be withdrawn.

        Returns:
            str: SQL query string to create the stored procedure `withdraw`.
        """
        return """
        CREATE OR REPLACE PROCEDURE withdraw(
            p_chat_id BIGINT,
            p_amount NUMERIC(20, 6)
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345; -- Shared lock ID for mutual exclusion
            v_balance NUMERIC(20, 6);
            v_new_balance NUMERIC(20, 6);
            v_firstname VARCHAR(100);
            v_lastname VARCHAR(100);
        BEGIN
            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(lock_id);

            BEGIN
                -- Fetch the current balance and client details
                SELECT balance, c.firstname, c.lastname
                INTO v_balance, v_firstname, v_lastname
                FROM balances b
                JOIN clients c ON b.chat_id = c.chat_id
                WHERE b.chat_id = p_chat_id;

                -- Calculate the new balance
                v_new_balance := v_balance - p_amount;

                -- Update the balance
                UPDATE balances
                SET balance = v_new_balance, last_update_date = CURRENT_TIMESTAMP
                WHERE chat_id = p_chat_id;

                -- Insert the ledger entry
                INSERT INTO ledger (
                    transaction_type,
                    chat_id,
                    firstname,
                    lastname,
                    currency,
                    method,
                    amount,
                    timestamp
                )
                VALUES (
                    'Withdrawal',
                    p_chat_id,
                    v_firstname,
                    v_lastname,
                    'USDT',
                    'Tether USDT (ERC20)',
                    -p_amount,
                    CURRENT_TIMESTAMP
                );
            EXCEPTION WHEN OTHERS THEN
                -- Release the advisory lock in case of error
                PERFORM pg_advisory_unlock(lock_id);
                RAISE;
            END;

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(lock_id);
        END;
        $$;
        """


    @staticmethod
    def create_get_balance_function():
        """Returns SQL query string to create a function to retrieve balance information for a client.

        This static method generates an SQL query string that defines a function `get_balance` in PL/pgSQL language.
        The function retrieves the balance details including client's first name, last name, currency, current balance,
        and last update date from the 'balances' and 'clients' tables based on the provided `p_chat_id`.

        Args:
            p_chat_id (BIGINT): Chat ID of the client whose balance information is to be retrieved.

        Returns:
            str: SQL query string to create the function `get_balance`.
        """
        return """
        CREATE OR REPLACE FUNCTION get_balance(
            p_chat_id BIGINT
        )
        RETURNS TABLE (
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            currency VARCHAR(10),
            balance NUMERIC(20, 6),
            last_update_date TIMESTAMP
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345;
            v_firstname VARCHAR(100);
            v_lastname VARCHAR(100);
            v_currency VARCHAR(10);
            v_balance NUMERIC(20, 6);
            v_last_update_date TIMESTAMP;
        BEGIN
            PERFORM pg_advisory_lock(lock_id);
            SELECT c.firstname, c.lastname, b.currency, b.balance, b.last_update_date
            INTO v_firstname, v_lastname, v_currency, v_balance, v_last_update_date
            FROM balances b
            JOIN clients c ON b.chat_id = c.chat_id
            WHERE b.chat_id = p_chat_id;
            PERFORM pg_advisory_unlock(lock_id);
            RETURN QUERY SELECT v_firstname, v_lastname, v_currency, v_balance, v_last_update_date;
        END;
        $$;
        """


    @staticmethod
    def create_correct_balance_procedure():
        """Returns SQL query string to create a procedure to correct balance and log in ledger.

        This static method generates an SQL query string that defines a procedure `correct_balance` in PL/pgSQL language.
        The procedure corrects the balance for a client identified by `p_chat_id` by adding `p_amount` to the current balance
        in the 'balances' table. It also logs this correction as a ledger entry in the 'ledger' table with transaction type
        'Correction'. The procedure acquires and releases an advisory lock to ensure data consistency during the operation.

        Args:
            p_chat_id (BIGINT): Chat ID of the client whose balance is to be corrected.
            p_amount (NUMERIC(20, 6)): Amount to be added to the current balance.

        Returns:
            str: SQL query string to create the procedure `correct_balance`.
        """
        return """
        CREATE OR REPLACE PROCEDURE correct_balance(
            p_chat_id BIGINT,
            p_amount NUMERIC(20, 6)
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345; -- Shared lock ID for mutual exclusion
            v_balance NUMERIC(20, 6);
            v_new_balance NUMERIC(20, 6);
            v_firstname VARCHAR(100);
            v_lastname VARCHAR(100);
        BEGIN
            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(lock_id);

            -- Fetch the current balance and client details
            SELECT balance, c.firstname, c.lastname
            INTO v_balance, v_firstname, v_lastname
            FROM balances b
            JOIN clients c ON b.chat_id = c.chat_id
            WHERE b.chat_id = p_chat_id;

            -- Calculate the new balance
            v_new_balance := v_balance + p_amount;

            -- Update the balance
            UPDATE balances
            SET balance = v_new_balance, last_update_date = CURRENT_TIMESTAMP
            WHERE chat_id = p_chat_id;

            -- Insert the ledger entry
            INSERT INTO ledger (
                transaction_type,
                chat_id,
                firstname,
                lastname,
                currency,
                method,
                amount,
                timestamp
            )
            VALUES (
                'Correction',
                p_chat_id,
                v_firstname,
                v_lastname,
                'USDT',
                'Tether USDT (ERC20)',
                p_amount,
                CURRENT_TIMESTAMP
            );

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(lock_id);
        END;
        $$;
        """


    @staticmethod
    def create_statement_function():
        """Returns SQL query string to create a function to fetch transaction statements for a client.

        This static method generates an SQL query string that defines a function `statement` in PL/pgSQL language.
        The function retrieves transaction statements for a client identified by `p_chat_id` within a specified date range
        (`p_start_date` to `p_end_date`) from the 'ledger' table. It returns a table of transactions including primary key,
        transaction type, client details (firstname, lastname), transaction details (currency, method, amount, deposit_address,
        kraken_refid, kraken_time, kraken_txid, target_address, percentage, conclusion_date), and statement timestamp (st_timestamp).
        The function acquires and releases an advisory lock to ensure data consistency during the query execution.

        Args:
            p_chat_id (BIGINT): Chat ID of the client whose transaction statements are to be fetched.
            p_start_date (DATE): Start date of the statement period.
            p_end_date (DATE): End date of the statement period.

        Returns:
            str: SQL query string to create the function `statement`.
        """        
        return """
        CREATE OR REPLACE FUNCTION statement(
            p_chat_id BIGINT,
            p_start_date DATE,
            p_end_date DATE
        )
        RETURNS TABLE (
            primary_key BIGINT,
            transaction_type VARCHAR(20),
            chat_id BIGINT,
            firstname VARCHAR(100),
            lastname VARCHAR(100),
            currency VARCHAR(10),
            method VARCHAR(100),
            amount NUMERIC(20, 6),
            deposit_address VARCHAR(255),
            kraken_refid VARCHAR(100),
            kraken_time TIMESTAMP,
            kraken_txid VARCHAR(100),
            target_address VARCHAR(255),
            percentage NUMERIC(10, 2),
            conclusion_date TIMESTAMP,
            st_timestamp TIMESTAMP
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345;
        BEGIN
            PERFORM pg_advisory_lock(lock_id);
            RETURN QUERY
            SELECT
                primary_key,
                transaction_type,
                chat_id,
                firstname,
                lastname,
                currency,
                method,
                amount,
                deposit_address,
                kraken_refid,
                kraken_time,
                kraken_txid,
                target_address,
                percentage,
                conclusion_date,
                timestamp AS st_timestamp  -- Alias renamed here
            FROM ledger
            WHERE chat_id = p_chat_id
            AND timestamp BETWEEN p_start_date AND p_end_date
            ORDER BY st_timestamp;
            PERFORM pg_advisory_unlock(lock_id);
        END;
        $$;
        """
    

    @staticmethod
    def create_total_liabilities_procedure():
        """Returns SQL query string to create a stored procedure to calculate total liabilities.

        This static method generates an SQL query string that defines a stored procedure `total_liabilities` in PL/pgSQL language.
        The procedure calculates the total liabilities by summing up the balances from the 'balances' table.
        It acquires and releases an advisory lock to ensure data consistency during the calculation.

        Outputs:
            p_date (DATE): Current date when the procedure is executed.
            p_asset (VARCHAR(10)): Asset name, 'USDT'.
            p_method (VARCHAR(100)): Method name, 'Tether USDT (ERC20)'.
            p_total_balance (NUMERIC(20, 6)): Total balance of liabilities.

        Returns:
            str: SQL query string to create the stored procedure `total_liabilities`.
        """
        return """
        CREATE OR REPLACE PROCEDURE total_liabilities(OUT p_date DATE, OUT p_asset VARCHAR(10), OUT p_method VARCHAR(100), OUT p_total_balance NUMERIC(20, 6))
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345; -- Shared lock ID for mutual exclusion
            v_total_balance NUMERIC(20, 6);
        BEGIN
            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(lock_id);

            -- Calculate the total balance
            SELECT SUM(balance) INTO v_total_balance FROM balances;

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(lock_id);

            -- Set the output parameters
            p_date := CURRENT_DATE;
            p_asset := 'USDT';
            p_method := 'Tether USDT (ERC20)';
            p_total_balance := v_total_balance;
        END;
        $$;
        """


    @staticmethod
    def create_projected_balance_procedure():
        """Returns SQL query string to create a stored procedure to calculate projected balance.

        This static method generates an SQL query string that defines a stored procedure `projected_balance` in PL/pgSQL language.
        The procedure calculates the projected balance for a given client (`p_chat_id`) up to a specified target date (`p_target_date`).
        It compounds the balance with returns fetched from the 'returns' table, starting from the current date until the target date.
        The procedure ensures data consistency by acquiring and releasing an advisory lock (`pg_advisory_lock` and `pg_advisory_unlock`).

        Inputs:
            p_target_date (DATE): Target date for projecting the balance.
            p_chat_id (BIGINT): Client's chat ID for whom the projected balance is calculated.

        Outputs:
            p_firstname (VARCHAR(100)): First name of the client.
            p_lastname (VARCHAR(100)): Last name of the client.
            p_date_from (DATE): Start date of the projection, which is the current date.
            p_date_to (DATE): End date of the projection, which is the target date.
            p_projected_balance (NUMERIC(20, 6)): Projected balance calculated based on compound returns.

        Raises:
            EXCEPTION: Raised if the target date (`p_target_date`) is earlier than the current date (`CURRENT_DATE`).

        Returns:
            str: SQL query string to create the stored procedure `projected_balance`.
        """
        return """
        CREATE OR REPLACE PROCEDURE projected_balance(
            p_target_date DATE,
            p_chat_id BIGINT,
            OUT p_firstname VARCHAR(100),
            OUT p_lastname VARCHAR(100),
            OUT p_date_from DATE,
            OUT p_date_to DATE,
            OUT p_projected_balance NUMERIC(20, 6)
        )
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_id BIGINT := 12345; -- Shared lock ID for mutual exclusion
            v_balance NUMERIC(20, 6);
            v_firstname VARCHAR(100);
            v_lastname VARCHAR(100);
            v_return NUMERIC(12, 6);
            v_projected_balance NUMERIC(20, 6);
            v_current_date DATE := CURRENT_DATE;
        BEGIN
            -- Ensure the target date is today or in the future
            IF p_target_date < v_current_date THEN
                RAISE EXCEPTION 'Target date must be today or a future date';
            END IF;

            -- Acquire the advisory lock
            PERFORM pg_advisory_lock(lock_id);

            -- Fetch the current balance and client details
            SELECT balance, c.firstname, c.lastname
            INTO v_balance, v_firstname, v_lastname
            FROM balances b
            JOIN clients c ON b.chat_id = c.chat_id
            WHERE b.chat_id = p_chat_id;

            -- Initialize projected balance with current balance
            v_projected_balance := v_balance;

            -- Loop through each return from current date to target date and compound the balance
            FOR v_return IN
                SELECT returns
                FROM returns
                WHERE date >= v_current_date AND date <= p_target_date
                ORDER BY date
            LOOP
                v_projected_balance := v_projected_balance * (1 + v_return);
            END LOOP;

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(lock_id);

            -- Set the output parameters
            p_firstname := v_firstname;
            p_lastname := v_lastname;
            p_date_from := v_current_date;
            p_date_to := p_target_date;
            p_projected_balance := v_projected_balance;
        END;
        $$;
        """

# this script must be excuted to initialize database.
if __name__ == "__main__":
    # Instantiate a DBInit object for initializing the database
    db_initializer = DBInit()

    # Call the initialize_database method to perform database initialization
    db_initializer.initialize_database()