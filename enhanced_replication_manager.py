#!/usr/bin/env python3
"""
Fixed Enhanced SQL Server Replication Manager

This enhanced script manages replication between a master SQL Server instance and multiple
read replicas with automatic setup capabilities and proper data synchronization using MERGE.
"""

import json
import logging
import time
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor
import pyodbc
import pandas as pd
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Configuration for a database connection"""
    host: str
    port: int
    username: str
    password: str
    database: str
    name: Optional[str] = None


@dataclass
class TableConfig:
    """Configuration for a table to replicate"""
    table_name: str
    primary_key: str
    replicate: bool
    sync_mode: str  # 'full' or 'incremental'
    timestamp_column: Optional[str] = None


@dataclass
class SchemaConfig:
    """Configuration for a schema to replicate"""
    schema_name: str
    tables: List[TableConfig]


class EnhancedSQLServerReplicationManager:
    """Enhanced manager for SQL Server replication with auto-setup capabilities"""
    
    def __init__(self, config_file: str):
        """Initialize the enhanced replication manager with configuration"""
        self.config = self._load_config(config_file)
        self.master_config = self._parse_database_config(self.config['master_database'])
        self.replica_configs = [
            self._parse_database_config(replica) 
            for replica in self.config['replica_databases']
        ]
        self.schemas = self._parse_schemas_config(self.config['schemas_to_replicate'])
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Replication state tracking
        self.last_sync_times = {}
        self.running = False
        
        # Auto-setup flags
        self.auto_setup_replicas = self.config.get('replication', {}).get('auto_setup_replicas', True)
        self.create_missing_schemas = self.config.get('replication', {}).get('create_missing_schemas', True)
        self.create_missing_tables = self.config.get('replication', {}).get('create_missing_tables', True)
        
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading configuration file: {e}")
            sys.exit(1)
            
    def _parse_database_config(self, config: Dict[str, Any]) -> DatabaseConfig:
        """Parse database configuration"""
        return DatabaseConfig(
            host=config['host'],
            port=config['port'],
            username=config['username'],
            password=config['password'],
            database=config['database'],
            name=config.get('name')
        )
        
    def _parse_schemas_config(self, schemas: List[Dict[str, Any]]) -> List[SchemaConfig]:
        """Parse schemas configuration"""
        result = []
        for schema in schemas:
            tables = []
            for table in schema['tables']:
                tables.append(TableConfig(
                    table_name=table['table_name'],
                    primary_key=table['primary_key'],
                    replicate=table['replicate'],
                    sync_mode=table['sync_mode'],
                    timestamp_column=table.get('timestamp_column')
                ))
            result.append(SchemaConfig(
                schema_name=schema['schema_name'],
                tables=tables
            ))
        return result
        
    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('log_file', './logs/replication_enhanced.log')
        
        # Handle path differences between Docker and local environments
        if log_file.startswith('/app/') and not os.path.exists('/app'):
            log_file = log_file.replace('/app/', './').replace('/app', '.')
        
        # Create logs directory if it doesn't exist
        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
        except PermissionError:
            log_file = 'replication_enhanced.log'
            print(f"Warning: Could not create log directory, using current directory: {log_file}")
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
    def _get_connection_string(self, db_config: DatabaseConfig, database: Optional[str] = None) -> str:
        """Generate ODBC connection string"""
        db_name = database or db_config.database
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config.host},{db_config.port};"
            f"DATABASE={db_name};"
            f"UID={db_config.username};"
            f"PWD={db_config.password};"
            f"TrustServerCertificate=yes;"
        )
        
    def _get_connection(self, db_config: DatabaseConfig, database: Optional[str] = None) -> pyodbc.Connection:
        """Get database connection"""
        connection_string = self._get_connection_string(db_config, database)
        timeout = self.config.get('monitoring', {}).get('connection_timeout_seconds', 30)
        
        try:
            connection = pyodbc.connect(connection_string, timeout=timeout)
            connection.autocommit = True
            return connection
        except Exception as e:
            self.logger.error(f"Failed to connect to {db_config.host}: {e}")
            raise
            
    def _database_exists(self, db_config: DatabaseConfig, database_name: str) -> bool:
        """Check if a database exists"""
        try:
            with self._get_connection(db_config, 'master') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sys.databases WHERE name = ?", database_name)
                return cursor.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking database existence: {e}")
            return False
            
    def _create_database(self, db_config: DatabaseConfig, database_name: str):
        """Create a database if it doesn't exist"""
        try:
            if not self._database_exists(db_config, database_name):
                with self._get_connection(db_config, 'master') as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"CREATE DATABASE [{database_name}]")
                    self.logger.info(f"Created database: {database_name}")
            else:
                self.logger.info(f"Database already exists: {database_name}")
        except Exception as e:
            self.logger.error(f"Error creating database {database_name}: {e}")
            raise
            
    def _schema_exists(self, db_config: DatabaseConfig, schema_name: str) -> bool:
        """Check if a schema exists"""
        try:
            with self._get_connection(db_config) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?", schema_name)
                return cursor.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking schema existence: {e}")
            return False
            
    def _create_schema(self, db_config: DatabaseConfig, schema_name: str):
        """Create a schema if it doesn't exist"""
        try:
            if schema_name != 'dbo' and not self._schema_exists(db_config, schema_name):
                with self._get_connection(db_config) as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"CREATE SCHEMA [{schema_name}]")
                    self.logger.info(f"Created schema: {schema_name}")
            else:
                self.logger.debug(f"Schema already exists or is dbo: {schema_name}")
        except Exception as e:
            self.logger.error(f"Error creating schema {schema_name}: {e}")
            raise
            
    def _table_exists(self, db_config: DatabaseConfig, schema_name: str, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            with self._get_connection(db_config) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """, schema_name, table_name)
                return cursor.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}")
            return False
            
    def _get_table_structure(self, db_config: DatabaseConfig, schema_name: str, table_name: str) -> str:
        """Get CREATE TABLE statement for a table"""
        try:
            with self._get_connection(db_config) as conn:
                cursor = conn.cursor()
                
                # Get column information
                cursor.execute("""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, schema_name, table_name)
                
                columns = cursor.fetchall()
                
                if not columns:
                    raise Exception(f"Table {schema_name}.{table_name} not found")
                
                # Build CREATE TABLE statement
                create_sql = f"CREATE TABLE [{schema_name}].[{table_name}] (\\n"
                column_definitions = []
                
                for col in columns:
                    col_name = col[0]
                    data_type = col[1]
                    max_length = col[2]
                    precision = col[3]
                    scale = col[4]
                    is_nullable = col[5]
                    default_value = col[6]
                    is_identity = col[7]
                    
                    # Build column definition
                    col_def = f"    [{col_name}] {data_type}"
                    
                    # Add length/precision
                    if data_type in ['varchar', 'nvarchar', 'char', 'nchar'] and max_length:
                        if max_length == -1:
                            col_def += "(MAX)"
                        else:
                            col_def += f"({max_length})"
                    elif data_type in ['decimal', 'numeric'] and precision:
                        col_def += f"({precision},{scale or 0})"
                    
                    # Add identity
                    if is_identity:
                        col_def += " IDENTITY(1,1)"
                    
                    # Add nullable
                    if is_nullable == 'NO':
                        col_def += " NOT NULL"
                    else:
                        col_def += " NULL"
                    
                    # Add default
                    if default_value:
                        col_def += f" DEFAULT {default_value}"
                    
                    column_definitions.append(col_def)
                
                create_sql += ",\\n".join(column_definitions)
                print(create_sql)
                
                # Get primary key information
                cursor.execute("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                    AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, schema_name, table_name)
                
                pk_columns = [row[0] for row in cursor.fetchall()]
                if pk_columns:
                    pk_def = f",\\n    PRIMARY KEY ({', '.join([f'[{col}]' for col in pk_columns])})"
                    create_sql += pk_def
                
                create_sql += "\\n)"
                
                return create_sql
                
        except Exception as e:
            self.logger.error(f"Error getting table structure for {schema_name}.{table_name}: {e}")
            raise
            
    def _create_table_from_master(self, replica_config: DatabaseConfig, schema_name: str, table_name: str):
        """Create table in replica based on master structure"""
        try:
            # Get table structure from master
            create_sql = self._get_table_structure(self.master_config, schema_name, table_name)
            
            # Create table in replica
            with self._get_connection(replica_config) as conn:
                cursor = conn.cursor()
                cursor.execute(create_sql)
                self.logger.info(f"Created table {schema_name}.{table_name} in replica {replica_config.host}")
                
        except Exception as e:
            self.logger.error(f"Error creating table {schema_name}.{table_name} in replica: {e}")
            raise
            
    def _setup_replica_database(self, replica_config: DatabaseConfig):
        """Setup replica database with all required schemas and tables"""
        self.logger.info(f"Setting up replica database: {replica_config.host}")
        
        try:
            # Create database if it doesn't exist
            if self.auto_setup_replicas:
                self._create_database(replica_config, replica_config.database)
            
            # Create schemas and tables
            for schema_config in self.schemas:
                if self.create_missing_schemas:
                    self._create_schema(replica_config, schema_config.schema_name)
                
                for table_config in schema_config.tables:
                    if table_config.replicate and self.create_missing_tables:
                        if not self._table_exists(replica_config, schema_config.schema_name, table_config.table_name):
                            self._create_table_from_master(replica_config, schema_config.schema_name, table_config.table_name)
                            
        except Exception as e:
            self.logger.error(f"Error setting up replica database {replica_config.host}: {e}")
            raise
            
    def _test_connections(self) -> bool:
        """Test all database connections"""
        self.logger.info("Testing database connections...")
        
        # Test master connection
        try:
            with self._get_connection(self.master_config) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                self.logger.info(f"Master connection successful: {self.master_config.host}")
        except Exception as e:
            self.logger.error(f"Master connection failed: {e}")
            return False
            
        # Test replica connections
        for replica_config in self.replica_configs:
            try:
                with self._get_connection(replica_config) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    self.logger.info(f"Replica connection successful: {replica_config.host} {replica_config.port} {replica_config.username} {replica_config.password}")
            except Exception as e:
                self.logger.error(f"Replica connection failed ({replica_config.host}): {e}")
                return False
                
        return True
        
    def _get_table_data(self, db_config: DatabaseConfig, schema: str, table: str, 
                       where_clause: str = "") -> pd.DataFrame:
        """Get data from a table"""
        query = f"SELECT * FROM [{schema}].[{table}]"
        if where_clause:
            query += f" WHERE {where_clause}"
            
        try:
            with self._get_connection(db_config) as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            self.logger.error(f"Error fetching data from {schema}.{table}: {e}")
            raise
            
    def _merge_data(self, db_config: DatabaseConfig, schema: str, table: str, 
                   data: pd.DataFrame, table_config: TableConfig):
        """Merge data into a table using MERGE statement for proper insert/update handling"""
        if data.empty:
            return
            
        try:
            with self._get_connection(db_config) as conn:
                cursor = conn.cursor()
                
                # Check if table has identity column
                has_identity = False
                try:
                    cursor.execute(f"""
                        SELECT COLUMNPROPERTY(OBJECT_ID('[{schema}].[{table}]'), '{table_config.primary_key}', 'IsIdentity')
                    """)
                    result = cursor.fetchone()
                    has_identity = result and result[0] == 1
                except:
                    pass
                
                # Enable identity insert if needed
                if has_identity:
                    cursor.execute(f"SET IDENTITY_INSERT [{schema}].[{table}] ON")
                
                # Create temporary table
                temp_table = f"#{table}_temp"
                cursor.execute(f"""
                    SELECT TOP 0 * INTO {temp_table} FROM [{schema}].[{table}]
                """)
                
                # Insert data into temporary table
                columns = ', '.join([f'[{col}]' for col in data.columns])
                placeholders = ', '.join(['?' for _ in data.columns])
                insert_temp_query = f"INSERT INTO {temp_table} ({columns}) VALUES ({placeholders})"
                
                # Convert DataFrame to list of tuples
                data_tuples = [tuple(None if pd.isna(val) else val for val in row) for _, row in data.iterrows()]
                
                # Insert in batches
                batch_size = self.config.get('replication', {}).get('batch_size', 1000)
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i:i + batch_size]
                    cursor.executemany(insert_temp_query, batch)
                
                # Perform MERGE operation
                non_pk_columns = [col for col in data.columns if col != table_config.primary_key]
                update_set = ', '.join([f"target.[{col}] = source.[{col}]" for col in non_pk_columns])
                insert_columns = ', '.join([f"[{col}]" for col in data.columns])
                insert_values = ', '.join([f"source.[{col}]" for col in data.columns])
                
                merge_sql = f"""
                MERGE [{schema}].[{table}] AS target
                USING {temp_table} AS source ON target.[{table_config.primary_key}] = source.[{table_config.primary_key}]
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({insert_columns}) VALUES ({insert_values});
                """
                
                cursor.execute(merge_sql)
                cursor.execute(f"DROP TABLE {temp_table}")
                
                if has_identity:
                    cursor.execute(f"SET IDENTITY_INSERT [{schema}].[{table}] OFF")
                
                self.logger.info(f"Merged {len(data)} rows into {schema}.{table}")
                
        except Exception as e:
            self.logger.error(f"Error merging data into {schema}.{table}: {e}")
            raise
            
    def _sync_table_full(self, schema_config: SchemaConfig, table_config: TableConfig, 
                        replica_config: DatabaseConfig):
        """Perform full synchronization of a table"""
        self.logger.info(f"Full sync: {schema_config.schema_name}.{table_config.table_name} "
                        f"to {replica_config.host}")
        
        try:
            # Get all data from master
            master_data = self._get_table_data(
                self.master_config, 
                schema_config.schema_name, 
                table_config.table_name
            )
            
            # Clear replica table first
            with self._get_connection(replica_config) as conn:
                cursor = conn.cursor()
                cursor.execute(f"DELETE FROM [{schema_config.schema_name}].[{table_config.table_name}]")
                
            # Use merge to insert all data
            if not master_data.empty:
                self._merge_data(replica_config, schema_config.schema_name, 
                               table_config.table_name, master_data, table_config)
                               
        except Exception as e:
            self.logger.error(f"Error in full sync for {schema_config.schema_name}.{table_config.table_name}: {e}")
            raise
            
    def _sync_table_incremental(self, schema_config: SchemaConfig, table_config: TableConfig, 
                              replica_config: DatabaseConfig):
        """Perform incremental synchronization of a table"""
        if not table_config.timestamp_column:
            self.logger.warning(f"No timestamp column specified for incremental sync of "
                              f"{schema_config.schema_name}.{table_config.table_name}")
            return
            
        try:
            # Get last sync time
            table_key = f"{schema_config.schema_name}.{table_config.table_name}.{replica_config.host}"
            last_sync = self.last_sync_times.get(table_key, datetime.min)
            
            # Format timestamp for SQL Server
            timestamp_str = last_sync.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            where_clause = f"[{table_config.timestamp_column}] > '{timestamp_str}'"
            
            self.logger.info(f"Incremental sync: {schema_config.schema_name}.{table_config.table_name} "
                            f"to {replica_config.host} since {timestamp_str}")
            
            # Get incremental data from master
            master_data = self._get_table_data(
                self.master_config, 
                schema_config.schema_name, 
                table_config.table_name,
                where_clause
            )
            
            if not master_data.empty:
                # Use merge to handle inserts and updates properly
                self._merge_data(replica_config, schema_config.schema_name, 
                               table_config.table_name, master_data, table_config)
                
            # Update last sync time
            self.last_sync_times[table_key] = datetime.now()
            
        except Exception as e:
            self.logger.error(f"Error in incremental sync for {schema_config.schema_name}.{table_config.table_name}: {e}")
            raise
        
    def _sync_table(self, schema_config: SchemaConfig, table_config: TableConfig, 
                   replica_config: DatabaseConfig):
        """Synchronize a single table"""
        if not table_config.replicate:
            return
            
        try:
            # Ensure table exists in replica
            if not self._table_exists(replica_config, schema_config.schema_name, table_config.table_name):
                if self.create_missing_tables:
                    self._create_table_from_master(replica_config, schema_config.schema_name, table_config.table_name)
                else:
                    self.logger.error(f"Table {schema_config.schema_name}.{table_config.table_name} "
                                    f"does not exist in replica {replica_config.host}")
                    return
            
            # Perform synchronization
            if table_config.sync_mode == 'full':
                self._sync_table_full(schema_config, table_config, replica_config)
            elif table_config.sync_mode == 'incremental':
                self._sync_table_incremental(schema_config, table_config, replica_config)
            else:
                self.logger.error(f"Unknown sync mode: {table_config.sync_mode}")
                
        except Exception as e:
            self.logger.error(f"Error syncing table {schema_config.schema_name}."
                            f"{table_config.table_name}: {e}")
            
    def _sync_replica(self, replica_config: DatabaseConfig):
        """Synchronize all configured tables to a replica"""
        self.logger.info(f"Starting sync to replica: {replica_config.host}")
        
        try:
            # Setup replica if needed
            self._setup_replica_database(replica_config)
            
            # Sync all tables
            for schema_config in self.schemas:
                for table_config in schema_config.tables:
                    if table_config.replicate:
                        self._sync_table(schema_config, table_config, replica_config)
                        
        except Exception as e:
            self.logger.error(f"Error syncing replica {replica_config.host}: {e}")
                    
        self.logger.info(f"Completed sync to replica: {replica_config.host}")
        
    def _sync_all_replicas(self):
        """Synchronize all replicas"""
        self.logger.info("Starting replication cycle")
        
        # Use thread pool for parallel replica sync
        max_workers = min(len(self.replica_configs), 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._sync_replica, replica_config)
                for replica_config in self.replica_configs
            ]
            
            # Wait for all to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Replica sync failed: {e}")
                    
        self.logger.info("Completed replication cycle")
        
    def _health_check(self):
        """Perform health check on all databases"""
        health_interval = self.config.get('monitoring', {}).get('health_check_interval_seconds', 60)
        
        while self.running:
            try:
                self.logger.debug("Performing health check")
                
                # Check master
                with self._get_connection(self.master_config) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT @@SERVERNAME, GETDATE()")
                    
                # Check replicas
                for replica_config in self.replica_configs:
                    with self._get_connection(replica_config) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT @@SERVERNAME, GETDATE()")
                        
                self.logger.debug("Health check completed successfully")
                
            except Exception as e:
                self.logger.error(f"Health check failed: {e}")
                
            time.sleep(health_interval)
            
    def start(self):
        """Start the enhanced replication manager"""
        self.logger.info("Starting Enhanced SQL Server Replication Manager")
        
        # Test connections first
        if not self._test_connections():
            self.logger.error("Connection tests failed. Exiting.")
            sys.exit(1)
            
        # Setup all replica databases
        self.logger.info("Setting up replica databases...")
        for replica_config in self.replica_configs:
            try:
                self._setup_replica_database(replica_config)
            except Exception as e:
                self.logger.error(f"Failed to setup replica {replica_config.host}: {e}")
                sys.exit(1)
                
        self.running = True
        
        # Start health check thread
        health_thread = threading.Thread(target=self._health_check, daemon=True)
        health_thread.start()
        
        # Main replication loop
        sync_interval = self.config.get('replication', {}).get('sync_interval_seconds', 30)
        
        try:
            while self.running:
                start_time = time.time()
                
                self._sync_all_replicas()
                
                # Calculate sleep time
                elapsed = time.time() - start_time
                sleep_time = max(0, sync_interval - elapsed)
                
                if sleep_time > 0:
                    self.logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                else:
                    self.logger.warning(f"Sync took {elapsed:.2f}s, longer than interval {sync_interval}s")
                    
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            self.stop()
            
    def stop(self):
        """Stop the enhanced replication manager"""
        self.logger.info("Stopping Enhanced SQL Server Replication Manager")
        self.running = False


def main():
    """Main entry point"""
    config_file = os.environ.get('CONFIG_FILE', 'replication_config_enhanced.json')
    
    # Handle path differences between Docker and local environments
    if config_file.startswith('/app/') and not os.path.exists('/app'):
        config_file = config_file.replace('/app/', './').replace('/app', '.')
        if not os.path.exists(config_file):
            config_file = 'replication_config_enhanced.json'
    
    if not os.path.exists(config_file):
        print(f"Configuration file not found: {config_file}")
        print("Looking for configuration file in current directory...")
        for alt_config in ['replication_config_enhanced.json', 'replication_config.json', 'config.json']:
            if os.path.exists(alt_config):
                config_file = alt_config
                print(f"Found configuration file: {config_file}")
                break
        else:
            print("No configuration file found. Please ensure replication_config_enhanced.json exists.")
            sys.exit(1)
        
    manager = EnhancedSQLServerReplicationManager(config_file)
    manager.start()


if __name__ == "__main__":
    main()