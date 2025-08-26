import pyodbc
import json
import logging

# --- Configuration Loading ---
def load_config(config_path='replication_config_enhanced.json'):
    """Loads the replication configuration from a JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

# --- Database Connection ---
def get_db_connection(db_config):
    """Establishes a connection to the SQL Server database."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_config['host']},{db_config['port']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def get_row_count(conn, schema, table):
    """Gets the row count of a table."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        return cursor.fetchone()[0]
    except pyodbc.ProgrammingError:
        # Table might not exist yet on replica if snapshot hasn't applied
        return 0

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    config = load_config()
    
    master_config = config['master_database']
    replica_configs = config['replica_databases']
    
    try:
        # --- Connect to Master ---
        master_conn = get_db_connection(master_config)
        
        # --- Connect to Replicas ---
        replica_conns = {r['name']: get_db_connection(r) for r in replica_configs}
        
        logging.info("\n--- Verifying Replication Status ---")
        
        for schema_info in config['schemas_to_replicate']:
            schema = schema_info['schema_name']
            for table_info in schema_info['tables']:
                table = table_info['table_name']
                
                master_count = get_row_count(master_conn, schema, table)
                logging.info(f"\nTable: [{schema}].[{table}]")
                logging.info(f"  Master ({master_config['host']}:{master_config['port']}): {master_count} rows")
                
                for r_name, r_conn in replica_conns.items():
                    replica_count = get_row_count(r_conn, schema, table)
                    logging.info(f"  Replica '{r_name}' ({config['replica_databases'][0]['host']}:{config['replica_databases'][0]['port']}): {replica_count} rows")
                    if master_count == replica_count:
                        logging.info(f"    -> STATUS: SYNCED")
                    else:
                        logging.warning(f"    -> STATUS: NOT SYNCED (Master: {master_count}, Replica: {replica_count})")

        # --- Close Connections ---
        master_conn.close()
        for conn in replica_conns.values():
            conn.close()
            
    except pyodbc.Error as ex:
        logging.error(f"Database connection error during verification: {ex}")

