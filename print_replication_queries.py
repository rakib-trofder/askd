import json
import time

def print_sql(conn_string, sql_command, step_description=""):
    """Prints a SQL command without executing it."""
    print("=" * 80)
    if step_description:
        print(f"STEP: {step_description}")
    print("-" * 80)
    print(f"CONNECTION STRING: {conn_string}")
    print("-" * 80)
    print("SQL QUERY:")
    print(sql_command)
    print("=" * 80)
    print()

def print_server_name_query(conn_string, description=""):
    """Prints the server name query without executing it."""
    sql_command = "SELECT @@SERVERNAME;"
    print_sql(conn_string, sql_command, f"Get Server Name - {description}")

def print_replication_queries(config):
    """Prints all SQL Server Transactional Replication queries based on a JSON config."""
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
    
    print("\n" + "🔍 REPLICATION SETUP QUERIES - PREVIEW MODE 🔍".center(80, "="))
    print()
    
    # Get actual server names from master and replicas
    print("📋 GETTING SERVER NAMES".center(80, "-"))
    print_server_name_query(master_conn_str, "Master Server")
    
    # Simulate master server name (you'll need to replace this with actual value)
    master_server_name = f"{master_config['name']}"  # Fallback
    print(f"📝 NOTE: Master server name will be retrieved from query above")
    print(f"📝 Fallback server name: {master_server_name}")
    print()
    
    # Step 1: Create the database and tables on replicas
    print("📋 STEP 1: CREATE DATABASES AND TABLES ON REPLICAS".center(80, "-"))
    for replica_idx, replica in enumerate(replicas_config):
        # Updated connection strings with port
        replica_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={replica['host']},{replica['port']};UID={replica['username']};PWD={replica['password']}"
        replica_conn_str_db = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={replica['host']},{replica['port']};DATABASE={replica['database']};UID={replica['username']};PWD={replica['password']}"

        # Create database
        create_db_sql = f"CREATE DATABASE {replica['database']};"
        print_sql(replica_conn_str, create_db_sql, f"Create Database on Replica {replica_idx + 1}")

        # Get table schema from master and create on replicas
        for schema in schemas_config:
            for table in schema['tables']:
                get_schema_sql = f"""
                    SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = '{schema['schema_name']}' AND c.TABLE_NAME = '{table['table_name']}';
                """
                print_sql(master_conn_str_db, get_schema_sql, f"Get Schema for {schema['schema_name']}.{table['table_name']}")

                # Simulate column definitions (in real execution, this would be dynamic)
                create_table_sql = f"""
                        USE {replica['database']};
                        CREATE TABLE [{schema['schema_name']}].[{table['table_name']}] (
                            -- Column definitions will be dynamically generated based on master schema
                            -- Example: [ID] int IDENTITY(1,1) NOT NULL,
                            -- Example: [Name] nvarchar(255) NULL,
                            -- Example: [CreatedDate] datetime NOT NULL
                        );
                    """
                print_sql(replica_conn_str_db, create_table_sql, f"Create Table {schema['schema_name']}.{table['table_name']} on Replica {replica_idx + 1}")

    print("⏱️  NOTE: Script would wait 10 seconds for databases/tables to be ready...")
    print()

    # Step 2: Configure the Distributor on the master and create the distributor_admin login
    print("📋 STEP 2: CONFIGURE DISTRIBUTOR AND CREATE REPLICATION LOGIN".center(80, "-"))
    
    # Create the distributor_admin login and a user for the distribution database
    login_sql = f"""
        USE {master_config['database']};
        IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = 'distributor_admin')
        BEGIN
            CREATE LOGIN [distributor_admin] WITH PASSWORD = N'{distributor_password}', CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;
        END;
    """
    print_sql(master_conn_str, login_sql, "Create Distributor Admin Login")

    distributor_sql = f"""
        USE {master_config['database']};
        EXEC sp_adddistributor @distributor = '{master_server_name}',
                               @password = N'{distributor_password}';
        EXEC sp_adddistributiondb @database = N'distribution';
        EXEC sp_adddistpublisher @publisher = '{master_server_name}',
                                 @distribution_db = N'distribution',
                                 @security_mode = 0,
                                 @login = N'distributor_admin',
                                 @password = N'{distributor_password}';
    """
    print_sql(master_conn_str, distributor_sql, "Configure Distributor")

    # Step 3: Create the publication
    print("📋 STEP 3: CREATE PUBLICATION".center(80, "-"))
    publication_sql = f"""
        USE {master_config['database']};
        EXEC sp_addpublication @publication = N'AskdTransactionalPublication',
                               @description = N'Transactional publication of askd database.',
                               @sync_method = N'concurrent',
                               @repl_freq = N'continuous',
                               @status = N'active';
    """
    print_sql(master_conn_str_db, publication_sql, "Create Publication")

    # Step 4: Add articles (tables) to the publication
    print("📋 STEP 4: ADD ARTICLES TO PUBLICATION".center(80, "-"))
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
            print_sql(master_conn_str_db, article_sql, f"Add Article: {schema['schema_name']}.{table['table_name']}")

    # Step 5: Ensure distributor_admin login exists on each replica
    print("📋 STEP 5: CREATE DISTRIBUTOR ADMIN LOGIN ON REPLICAS".center(80, "-"))
    for replica_idx, replica in enumerate(replicas_config):
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
        print_sql(replica_conn_str, login_sql_replica, f"Create Distributor Admin on Replica {replica_idx + 1}")

    # Step 6: Add subscriptions for each replica
    print("📋 STEP 6: ADD SUBSCRIPTIONS FOR REPLICAS".center(80, "-"))
    for replica in replicas_config:
        replica_server_name = replica['name']
        subscription_sql = f"""
            USE {master_config['database']};
            EXEC sp_addsubscription @publication = N'AskdTransactionalPublication',
                                     @subscriber = '{replica_server_name}',
                                     @destination_db = N'{replica['database']}',
                                     @subscription_type = N'Push',
                                     @sync_type = N'automatic',
                                     @article = 'all';

            EXEC sp_addpushsubscription_agent @publication = N'AskdTransactionalPublication',
                                               @subscriber = '{replica_server_name}',
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
        print_sql(master_conn_str_db, subscription_sql, f"Add Subscription for Replica ({replica_server_name})")

    print("✅ REPLICATION SETUP QUERIES PREVIEW COMPLETED!".center(80, "="))
    print()
    print("📋 SUMMARY:")
    print(f"   • Master Database: {master_config['database']} on {master_config['host']}:{master_config['port']}")
    print(f"   • Number of Replicas: {len(replicas_config)}")
    print(f"   • Sync Interval: {sync_interval} seconds")
    print(f"   • Publication Name: AskdTransactionalPublication")
    print(f"   • Total Schemas: {len(schemas_config)}")
    total_tables = sum(len(schema['tables']) for schema in schemas_config)
    print(f"   • Total Tables: {total_tables}")
    print()
    print("⚠️  NOTE: These are the queries that would be executed.")
    print("⚠️  Remember to ensure SQL Server Agent is running before executing actual replication setup!")

if __name__ == "__main__":
    try:
        with open('replication_config_enhanced.json', 'r') as f:
            config = json.load(f)
        print_replication_queries(config)
    except FileNotFoundError:
        print("Error: replication_config_enhanced.json not found.")
        print("Please ensure the configuration file exists in the same directory.")
    except Exception as e:
        print(f"An error occurred: {e}")