import json
import pyodbc
import logging
import time
import os

# --- Configuration Loading ---
def load_config(config_path='replication_config_enhanced.json'):
    """Loads the replication configuration from a JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

# --- Logging Setup ---
def setup_logging(config):
    """Configures logging based on the provided configuration."""
    log_config = config['logging']
    log_dir = os.path.dirname(log_config['log_file'])
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    logging.basicConfig(
        level=getattr(logging, log_config['level'].upper(), logging.INFO),
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_config['log_file'],
        filemode='a'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

# --- Database Connection ---
def get_db_connection(db_config, timeout=30):
    """Establishes a connection to the SQL Server database."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_config['host']},{db_config['port']};"
        f"DATABASE={db_config.get('database', 'master')};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
        f"Timeout={timeout};"
    )
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        logging.info(f"Successfully connected to {db_config['host']}:{db_config['port']}")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logging.error(f"Error connecting to {db_config['host']}:{db_config['port']}. SQLSTATE: {sqlstate}")
        logging.error(ex)
        return None

# --- Replication Setup Functions ---
def execute_sql(conn, sql, params=None):
    """Executes a SQL command with retry logic."""
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        logging.info(f"Executed SQL: {sql.strip()}")
        return cursor
    except pyodbc.Error as ex:
        logging.error(f"Error executing SQL: {sql.strip()}")
        logging.error(ex)
        raise

def setup_master_and_replicas(config):
    """Main function to orchestrate the replication setup."""
    master_config = config['master_database']
    
    # --- Connect to Master ---
    master_conn = get_db_connection(master_config)
    if not master_conn:
        logging.critical("Could not connect to master database. Aborting setup.")
        return

    # --- Create Database and Schemas on Master ---
    db_name = master_config['database']
    execute_sql(master_conn, f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{db_name}') CREATE DATABASE {db_name};")
    execute_sql(master_conn, f"USE {db_name};")
    
    for schema_info in config['schemas_to_replicate']:
        schema_name = schema_info['schema_name']
        execute_sql(master_conn, f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}') EXEC('CREATE SCHEMA {schema_name}');")

    # --- Create Tables on Master ---
    for schema_info in config['schemas_to_replicate']:
        schema_name = schema_info['schema_name']
        for table in schema_info['tables']:
            table_name = table['table_name']
            pk_column = table['primary_key']
            ts_column = table['timestamp_column']
            
            # A more robust table creation for demonstration
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{schema_name}].[{table_name}]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [{schema_name}].[{table_name}](
                    [{pk_column}] [int] IDENTITY(1,1) NOT NULL,
                    [Data] [nvarchar](100) NULL,
                    [{ts_column}] [datetime] NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT [PK_{table_name}] PRIMARY KEY CLUSTERED ([{pk_column}] ASC)
                );
                ALTER TABLE [{schema_name}].[{table_name}] ADD CONSTRAINT DF_{table_name}_{ts_column} DEFAULT GETDATE() FOR [{ts_column}];
            END
            """
            execute_sql(master_conn, create_table_sql)

    # --- Configure Distributor on Master ---
    logging.info("Configuring the Distributor...")
    dist_password = config['replication']['distributor_admin_password']
    execute_sql(master_conn, "USE master;")
    execute_sql(master_conn, f"""
        IF NOT EXISTS (SELECT * FROM sys.servers WHERE name = @@SERVERNAME AND data_source = @@SERVERNAME)
        BEGIN
            EXEC sp_adddistributor @distributor = @@SERVERNAME, @password = '{dist_password}';
            EXEC sp_adddistributiondb @database = 'distribution';
        END
    """)

    # --- Create Publication on Master ---
    publication_name = f"{db_name}_Pub"
    execute_sql(master_conn, f"USE {db_name};")
    execute_sql(master_conn, f"""
        IF NOT EXISTS (SELECT * FROM sys.publications WHERE name = '{publication_name}')
        BEGIN
            EXEC sp_addpublication @publication = '{publication_name}', @status = 'active';
        END
    """)
    
    # --- Add Articles to Publication ---
    for schema_info in config['schemas_to_replicate']:
        for table in schema_info['tables']:
            if table['replicate']:
                table_name = table['table_name']
                schema_name = schema_info['schema_name']
                logging.info(f"Adding article for table: {schema_name}.{table_name}")
                execute_sql(master_conn, f"""
                    IF NOT EXISTS (SELECT * FROM sys.articles WHERE name = '{table_name}' AND pubid = (SELECT pubid FROM sys.publications WHERE name = '{publication_name}'))
                    BEGIN
                        EXEC sp_addarticle 
                            @publication = '{publication_name}', 
                            @article = '{table_name}', 
                            @source_object = '{table_name}',
                            @source_owner = '{schema_name}';
                    END
                """)

    # --- Start Snapshot Agent ---
    execute_sql(master_conn, f"EXEC sp_startpublication_snapshot @publication = '{publication_name}';")
    logging.info("Snapshot agent started for the publication.")

    # --- Setup Replicas ---
    for replica_config in config['replica_databases']:
        logging.info(f"--- Setting up Replica: {replica_config['name']} ---")
        replica_conn = get_db_connection(replica_config)
        if not replica_conn:
            logging.error(f"Skipping replica {replica_config['name']} due to connection failure.")
            continue
        
        # Create database on replica
        execute_sql(replica_conn, f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{db_name}') CREATE DATABASE {db_name};")

        # Add subscription
        execute_sql(replica_conn, f"USE {db_name};")
        execute_sql(replica_conn, f"""
            IF NOT EXISTS (SELECT * FROM sys.subscriptions WHERE srvname = '{master_config['host']},{master_config['port']}')
            BEGIN
                EXEC sp_addsubscription 
                    @publication = '{publication_name}', 
                    @subscriber = @@SERVERNAME, 
                    @destination_db = '{db_name}',
                    @subscription_type = 'Push',
                    @sync_type = 'automatic';
            END
        """)
        logging.info(f"Subscription added for replica: {replica_config['name']}")

    logging.info("Replication setup script completed.")
    master_conn.close()


if __name__ == "__main__":
    config = load_config()
    setup_logging(config)
    if config['replication']['enabled']:
        setup_master_and_replicas(config)
    else:
        logging.info("Replication is disabled in the configuration file.")
