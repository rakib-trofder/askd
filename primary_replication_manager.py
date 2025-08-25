import pyodbc
import json
import time

def execute_sql(conn_string, sql_command, params=None):
    """Executes a SQL command with error handling."""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        if params:
            cursor.execute(sql_command, params)
        else:
            cursor.execute(sql_command)
        print(f"SQL command executed successfully: {sql_command.splitlines()[0]}...")
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"SQL Error: {sqlstate}")
        print(ex.args[1])
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def setup_replication(config):
    """Sets up SQL Server Transactional Replication based on a JSON config."""
    master_config = config['master_database']
    replicas_config = config['replica_databases']
    schemas_config = config['schemas_to_replicate']
    replication_config = config['replication']
    
    # Get sync interval and distributor admin password from configuration
    sync_interval = replication_config.get('sync_interval_seconds', 15)
    distributor_password = replication_config.get('distributor_admin_password')

    # Updated connection strings with port
    master_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={master_config['host']},{master_config['port']};UID={master_config['username']};PWD={master_config['password']}"
    master_conn_str_db = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={master_config['host']},{master_config['port']};DATABASE={master_config['database']};UID={master_config['username']};PWD={master_config['password']}"

    # Step 1: Create the database and tables on replicas
    print("\n--- Creating databases and tables on replicas ---")
    for replica in replicas_config:
        # Updated connection strings with port
        replica_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={replica['host']},{replica['port']};UID={replica['username']};PWD={replica['password']}"
        replica_conn_str_db = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={replica['host']},{replica['port']};DATABASE={replica['database']};UID={replica['username']};PWD={replica['password']}"

        # Create database
        create_db_sql = f"CREATE DATABASE {replica['database']};"
        execute_sql(replica_conn_str, create_db_sql)

        # Get table schema from master and create on replicas
        for schema in schemas_config:
            for table in schema['tables']:
                get_schema_sql = f"""
                    SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = '{schema['schema_name']}' AND c.TABLE_NAME = '{table['table_name']}';
                """
                conn = pyodbc.connect(master_conn_str_db)
                cursor = conn.cursor()
                cursor.execute(get_schema_sql)
                columns = cursor.fetchall()
                conn.close()

                if columns:
                    column_defs = []
                    for col in columns:
                        col_name, data_type, max_len, is_nullable = col
                        col_def = f"[{col_name}] {data_type}"
                        if max_len is not None and data_type in ['nvarchar', 'varchar', 'char', 'nchar']:
                            col_def += f"({max_len})"
                        elif data_type in ['decimal', 'numeric']:
                            col_def += "(18, 2)"
                        col_def += " NOT NULL" if is_nullable == "NO" else " NULL"
                        column_defs.append(col_def)

                    create_table_sql = f"""
                        USE {replica['database']};
                        CREATE TABLE [{schema['schema_name']}].[{table['table_name']}] ({', '.join(column_defs)});
                    """
                    execute_sql(replica_conn_str_db, create_table_sql)

    # Wait for databases/tables to be ready
    print("\nWaiting for databases and tables to be ready...")
    time.sleep(10)

    # Step 2: Configure the Distributor on the master and create the distributor_admin login
    print("\n--- Configuring Distributor and creating replication login ---")
    
    # Create the distributor_admin login and a user for the distribution database
    login_sql = f"""
        USE master;
        IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = 'distributor_admin')
        BEGIN
            CREATE LOGIN [distributor_admin] WITH PASSWORD = N'{distributor_password}', CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;
        END;
    """
    execute_sql(master_conn_str, login_sql)

    distributor_sql = f"""
        USE master;
        EXEC sp_adddistributor @distributor = N'{master_config['host']},{master_config['port']}',
                               @password = N'{distributor_password}';
        EXEC sp_adddistributiondb @database = N'distribution';
        EXEC sp_adddistpublisher @publisher = N'{master_config['host']},{master_config['port']}',
                                 @distribution_db = N'distribution',
                                 @security_mode = 0,
                                 @login = N'distributor_admin',
                                 @password = N'{distributor_password}';
    """
    execute_sql(master_conn_str, distributor_sql)

    # Step 3: Create the publication
    print("\n--- Creating Publication ---")
    publication_sql = f"""
        USE {master_config['database']};
        EXEC sp_addpublication @publication = N'AskdTransactionalPublication',
                               @description = N'Transactional publication of askd database.',
                               @sync_method = N'concurrent',
                               @repl_freq = N'continuous',
                               @status = N'active';
    """
    execute_sql(master_conn_str_db, publication_sql)

    # Step 4: Add articles (tables) to the publication
    print("\n--- Adding Articles to Publication ---")
    for schema in schemas_config:
        for table in schema['tables']:
            article_sql = f"""
                USE {master_config['database']};
                EXEC sp_addarticle @publication = N'AskdTransactionalPublication',
                                   @article = N'{table['table_name']}',
                                   @source_object = N'{table['table_name']}',
                                   @source_owner = N'{schema['schema_name']}',
                                   @destination_table = N'{table['table_name']}';
            """
            execute_sql(master_conn_str_db, article_sql)

    # Step 5: Ensure distributor_admin login exists on each replica
    print("\n--- Ensuring distributor_admin login exists on replicas ---")
    for replica in replicas_config:
        replica_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={replica['host']},{replica['port']};UID={replica['username']};PWD={replica['password']}"
        login_sql_replica = f"""
            USE master;
            IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = 'distributor_admin')
            BEGIN
                CREATE LOGIN [distributor_admin] WITH PASSWORD = N'{distributor_password}', CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;
            END;
            USE {replica['database']};
            IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = 'distributor_admin')
            BEGIN
                CREATE USER [distributor_admin] FOR LOGIN [distributor_admin];
                EXEC sp_addrolemember N'db_owner', N'distributor_admin';
            END;
        """
        execute_sql(replica_conn_str, login_sql_replica)

    # Step 6: Add subscriptions for each replica
    print("\n--- Adding Subscriptions for Replicas ---")
    for replica in replicas_config:
        subscription_sql = f"""
            USE {master_config['database']};
            EXEC sp_addsubscription @publication = N'AskdTransactionalPublication',
                                     @subscriber = N'{replica['host']},{replica['port']}',
                                     @destination_db = N'{replica['database']}',
                                     @subscription_type = N'Push',
                                     @sync_type = N'automatic',
                                     @article = 'all';

            EXEC sp_addpushsubscription_agent @publication = N'AskdTransactionalPublication',
                                               @subscriber = N'{replica['host']},{replica['port']}',
                                               @subscriber_db = N'{replica['database']}',
                                               @job_login = N'distributor_admin',
                                               @job_password = N'{distributor_password}',
                                               @subscriber_security_mode = 1,
                                               @frequency_type = 4,
                                               @frequency_interval = 1,
                                               @frequency_relative_interval = 1,
                                               @frequency_recurrence_factor = 0,
                                               @frequency_subday = 4,
                                               @frequency_subday_interval = {sync_interval};
        """
        print(subscription_sql)
        execute_sql(master_conn_str_db, subscription_sql)

if __name__ == "__main__":
    try:
        with open('replication_config_enhanced.json', 'r') as f:
            config = json.load(f)
        setup_replication(config)
        print("\nReplication setup completed successfully! 🎉")
        print("Note: The SQL Server Agent must be running on the master for continuous replication.")
    except FileNotFoundError:
        print("Error: replication_config_enhanced.json not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
