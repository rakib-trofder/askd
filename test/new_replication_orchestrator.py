
import json
import logging
import time
import sys
import os
import pyodbc
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor

class NewReplicationOrchestrator:
    """
    A new implementation for database replication based on a dynamic JSON config.
    """

    def __init__(self, config_path: str):
        """
        Initializes the orchestrator by loading and parsing the configuration.
        """
        self.config = self._load_config(config_path)
        self._setup_logging()
        
        # Database configurations
        self.master_db = self.config['master_database']
        self.replicas_db = self.config['replica_databases']
        
        # Replication settings
        self.replication_settings = self.config['replication']
        self.schemas_to_replicate = self.config['schemas_to_replicate']
        
        # State tracking
        self.last_sync_timestamps = {}
        self.is_running = True

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Loads the JSON configuration file."""
        self.log(f"Loading configuration from {config_path}", "INFO")
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.log(f"Configuration file not found at {config_path}", "ERROR")
            sys.exit(1)
        except json.JSONDecodeError:
            self.log(f"Invalid JSON in configuration file: {config_path}", "ERROR")
            sys.exit(1)

    def _setup_logging(self):
        """Configures logging based on the settings in the config file."""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO').upper()
        log_file = log_config.get('log_file', 'replication.log')
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.log("Logging configured.", "INFO")

    def log(self, message: str, level: str = "INFO"):
        """Helper method for logging."""
        if level == "INFO":
            logging.info(message)
        elif level == "ERROR":
            logging.error(message)
        elif level == "WARNING":
            logging.warning(message)
        elif level == "DEBUG":
            logging.debug(message)

    def _create_connection(self, db_config: Dict[str, Any]) -> Optional[pyodbc.Connection]:
        """Creates and returns a pyodbc connection."""
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_config['host']},{db_config['port']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};"
                f"PWD={db_config['password']};"
                f"TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(conn_str, autocommit=False)
            self.log(f"Successfully connected to {db_config['host']}:{db_config['port']}", "DEBUG")
            return conn
        except Exception as e:
            self.log(f"Failed to connect to {db_config['host']}. Error: {e}", "ERROR")
            return None

    def run(self):
        """Starts the main replication loop."""
        sync_interval = self.replication_settings.get('sync_interval_seconds', 15)
        self.log(f"Starting replication orchestrator. Sync interval: {sync_interval} seconds.")
        
        while self.is_running:
            try:
                start_time = time.time()
                self.log("Starting new replication cycle.", "INFO")
                
                self.replicate_all()
                
                end_time = time.time()
                duration = end_time - start_time
                self.log(f"Replication cycle finished in {duration:.2f} seconds.", "INFO")
                
                sleep_time = max(0, sync_interval - duration)
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                self.log("Shutdown signal received. Stopping orchestrator.", "INFO")
                self.is_running = False
            except Exception as e:
                self.log(f"An unexpected error occurred in the main loop: {e}", "ERROR")
                time.sleep(sync_interval) # Wait before retrying

    def replicate_all(self):
        """
        Orchestrates the replication for all schemas and tables to all replicas.
        """
        for schema_config in self.schemas_to_replicate:
            schema_name = schema_config['schema_name']
            for table_config in schema_config['tables']:
                if table_config.get('replicate', False):
                    self.replicate_table(schema_name, table_config)

    def replicate_table(self, schema_name: str, table_config: Dict[str, Any]):
        """
        Replicates a single table's data from master to all replicas.
        """
        table_name = table_config['table_name']
        sync_mode = table_config.get('sync_mode', 'incremental')
        
        self.log(f"Processing table: {schema_name}.{table_name} (Mode: {sync_mode})")

        # 1. Fetch data from Master
        master_conn = self._create_connection(self.master_db)
        if not master_conn:
            self.log(f"Cannot replicate {schema_name}.{table_name}, failed to connect to master.", "ERROR")
            return

        try:
            data_df = self._fetch_master_data(master_conn, schema_name, table_config)
            if data_df is None:
                # Error already logged in fetch method
                return
            
            if data_df.empty:
                self.log(f"No new data to replicate for {schema_name}.{table_name}.", "INFO")
                # We might still need to process deletes
            else:
                self.log(f"Fetched {len(data_df)} rows from master for {schema_name}.{table_name}.", "INFO")

            # 2. Apply data to all replicas in parallel
            with ThreadPoolExecutor(max_workers=len(self.replicas_db)) as executor:
                futures = [
                    executor.submit(self._apply_to_replica, replica_db, schema_name, table_config, data_df)
                    for replica_db in self.replicas_db
                ]
                for future in futures:
                    future.result() # Wait for all replicas to complete

            # 3. Update sync timestamp if successful
            table_key = f"{schema_name}.{table_name}"
            self.last_sync_timestamps[table_key] = datetime.utcnow()

        finally:
            master_conn.close()

    def _fetch_master_data(self, master_conn: pyodbc.Connection, schema_name: str, table_config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Fetches incremental or full data from the master table."""
        table_name = table_config['table_name']
        timestamp_col = table_config.get('timestamp_column')
        table_key = f"{schema_name}.{table_name}"
        
        query = f"SELECT * FROM [{schema_name}].[{table_name}]"
        
        if table_config['sync_mode'] == 'incremental' and timestamp_col:
            last_sync = self.last_sync_timestamps.get(table_key)
            if last_sync:
                # Note: Using parameter markers is safer
                query += f" WHERE [{timestamp_col}] > '{last_sync.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"

        try:
            data_df = pd.read_sql(query, master_conn)
            return data_df
        except Exception as e:
            self.log(f"Error fetching data from {table_key} on master: {e}", "ERROR")
            return None

    def _apply_to_replica(self, replica_config: Dict[str, Any], schema_name: str, table_config: Dict[str, Any], data_df: pd.DataFrame):
        """Applies the fetched data to a single replica using a MERGE strategy."""
        replica_name = replica_config.get('name', replica_config['host'])
        table_name = table_config['table_name']
        pk_column = table_config['primary_key']
        
        self.log(f"Applying data for {schema_name}.{table_name} to replica '{replica_name}'.")
        
        replica_conn = self._create_connection(replica_config)
        if not replica_conn:
            self.log(f"Skipping replica '{replica_name}' due to connection failure.", "ERROR")
            return

        cursor = replica_conn.cursor()
        temp_table_name = f"##{table_name}_temp_{int(time.time())}"

        try:
            # Create a temporary table with the same schema as the target
            cursor.execute(f"SELECT TOP 0 * INTO {temp_table_name} FROM [{schema_name}].[{table_name}];")

            # Bulk insert data from the DataFrame into the temporary table
            if not data_df.empty:
                # Prepare data for fast insertion
                data_tuples = [tuple(row) for row in data_df.itertuples(index=False)]
                cols_sql = ','.join([f'[{col}]' for col in data_df.columns])
                placeholders = ','.join(['?'] * len(data_df.columns))
                
                insert_sql = f"INSERT INTO {temp_table_name} ({cols_sql}) VALUES ({placeholders})"
                
                # Execute in batches
                batch_size = self.replication_settings.get('batch_size', 1000)
                cursor.fast_executemany = True
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i:i+batch_size]
                    cursor.executemany(insert_sql, batch)
                self.log(f"Bulk inserted {len(data_tuples)} rows into temp table on replica '{replica_name}'.", "DEBUG")

            # Use MERGE to synchronize the target table with the temp table
            all_cols = [f'[{c}]' for c in data_df.columns]
            non_pk_cols = [f'[{c}]' for c in data_df.columns if c != pk_column]
            
            update_set_sql = ', '.join([f"T.[{col}] = S.[{col}]" for col in non_pk_cols])
            insert_cols_sql = ', '.join(all_cols)
            insert_vals_sql = ', '.join([f"S.{col}" for col in all_cols])

            merge_sql = f"""
            MERGE [{schema_name}].[{table_name}] AS T
            USING {temp_table_name} AS S
            ON (T.[{pk_column}] = S.[{pk_column}])
            WHEN MATCHED THEN
                UPDATE SET {update_set_sql}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
            """
            
            if self.replication_settings.get('replicate_deletes', False) and table_config['sync_mode'] == 'full':
                 merge_sql += """
                 WHEN NOT MATCHED BY SOURCE THEN
                    DELETE
                 """

            cursor.execute(merge_sql)
            replica_conn.commit()
            self.log(f"Successfully merged data into {schema_name}.{table_name} on replica '{replica_name}'. Rows affected: {cursor.rowcount}", "INFO")

        except Exception as e:
            replica_conn.rollback()
            self.log(f"Error applying data to replica '{replica_name}': {e}", "ERROR")
        finally:
            # Clean up the temporary table
            try:
                cursor.execute(f"DROP TABLE {temp_table_name};")
                replica_conn.commit()
            except Exception as e:
                self.log(f"Could not drop temp table {temp_table_name} on replica '{replica_name}': {e}", "WARNING")
            replica_conn.close()


if __name__ == "__main__":
    config_file = 'replication_config_enhanced.json'
    if not os.path.exists(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        sys.exit(1)
        
    orchestrator = NewReplicationOrchestrator(config_file)
    orchestrator.run()
