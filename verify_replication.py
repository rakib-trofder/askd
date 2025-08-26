import pyodbc
import json
import logging
import time

# --- Configuration Loading ---
def load_config(config_path='replication_config_enhanced.json'):
    """Loads the replication configuration from a JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

# --- Database Connection and Querying ---
def get_row_count(conn_str, db_name, schema, table):
    """Connects to a database and gets the row count of a specific table."""
    full_conn_str = f"{conn_str};DATABASE={db_name}"
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(full_conn_str)
        cursor = conn.cursor()
        # Use NOLOCK hint to avoid blocking on replicas
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}] WITH (NOLOCK);"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        return count
    except pyodbc.ProgrammingError as ex:
        # This can happen if the table doesn't exist yet on the replica
        logging.warning(f"Could not query table [{schema}].[{table}] on {db_name}. It might not have been created yet. Error: {ex}")
        return 0
    except pyodbc.Error as ex:
        logging.error(f"Database error while getting row count for {schema}.{table}: {ex}")
        return -1 # Indicate an error
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# --- Main Verification Logic ---
def verify_replication(config):
    """Compares row counts between the master and replica databases."""
    master_config = config['master_database']
    replicas_config = config['replica_databases']
    schemas_config = config['schemas_to_replicate']

    # --- Build Connection Strings ---
    master_conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={master_config['host']},{master_config['port']};"
        f"UID={master_config['username']};"
        f"PWD={master_config['password']};"
        f"TrustServerCertificate=yes;"
    )

    logging.info("--- Starting Replication Verification ---")
    # Give the replication a moment to catch up
    logging.info("Waiting for 10 seconds to allow for data synchronization...")
    time.sleep(10)

    for schema_info in schemas_config:
        schema = schema_info['schema_name']
        for table_info in schema_info['tables']:
            table = table_info['table_name']
            
            logging.info(f"\nVerifying Table: [{schema}].[{table}]")
            
            # --- Get Master Row Count ---
            master_count = get_row_count(master_conn_str, master_config['database'], schema, table)
            if master_count == -1:
                logging.error(f"Could not retrieve row count from master for {schema}.{table}. Skipping.")
                continue
            
            logging.info(f"  Master ({master_config['host']}:{master_config['port']}) -> Rows: {master_count}")

            # --- Get Replica Row Counts ---
            for replica in replicas_config:
                replica_conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={replica['host']},{replica['port']};"
                    f"UID={replica['username']};"
                    f"PWD={replica['password']};"
                    f"TrustServerCertificate=yes;"
                )
                
                replica_count = get_row_count(replica_conn_str, replica['database'], schema, table)
                
                status = "SYNCED ✅" if master_count == replica_count else "NOT SYNCED ❌"
                logging.info(f"  Replica '{replica['name']}' ({replica['host']}:{replica['port']}) -> Rows: {replica_count} | Status: {status}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        config = load_config()
        verify_replication(config)
        logging.info("\n--- Verification Complete ---")
    except FileNotFoundError:
        logging.error("Error: replication_config_enhanced.json not found.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
