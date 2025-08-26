import pyodbc
import json
import logging
import time
from datetime import datetime

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

def insert_data(conn, schema, table, pk, ts_col, count):
    """Inserts sample data into a table."""
    cursor = conn.cursor()
    logging.info(f"Inserting {count} records into {schema}.{table}...")
    for i in range(count):
        data = f"Sample Data {i + 1} for {table} at {datetime.now()}"
        # The timestamp column will be updated by the default constraint
        cursor.execute(f"INSERT INTO [{schema}].[{table}] (Data) VALUES (?)", data)
    conn.commit()
    logging.info(f"Finished inserting data into {schema}.{table}.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    config = load_config()
    master_config = config['master_database']
    
    try:
        master_conn = get_db_connection(master_config)
        
        for schema_info in config['schemas_to_replicate']:
            schema = schema_info['schema_name']
            for table_info in schema_info['tables']:
                table = table_info['table_name']
                pk = table_info['primary_key']
                ts_col = table_info['timestamp_column']
                insert_data(master_conn, schema, table, pk, ts_col, 100) # Insert 100 rows per table
        
        master_conn.close()
        logging.info("Sample data insertion complete.")
        
    except pyodbc.Error as ex:
        logging.error(f"Database error during data insertion: {ex}")

