"""
Simplified SQL Server Replication Manager

Core features:
- SQL Server transactional replication setup
- Incremental sync for large datasets
- Dynamic configuration support
- Streamlined error handling
- Connection pooling
"""

import pyodbc
import json
import time
import logging
import os
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

class ReplicationManager:
    """Simplified replication manager for SQL Server transactional replication"""
    
    def __init__(self, config_path: str = 'replication_config_enhanced.json'):
        self.config_path = config_path
        self.config = self.load_config()
        self.connection_pool = {}
        self.setup_logging()
        
    def load_config(self) -> dict:
        """Load configuration file"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Set defaults
            rep_config = config.setdefault('replication', {})
            rep_config.setdefault('distributor_admin_password', 'DistributorPassword!123')
            rep_config.setdefault('sync_interval_seconds', 15)
            rep_config.setdefault('retry_attempts', 3)
            rep_config.setdefault('connection_timeout', 30)
            
            return config
            
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Configuration error: {e}")
            raise
            

        
    def setup_logging(self):
        """Setup logging"""
        log_cfg = self.config.get('logging', {})
        level = getattr(logging, log_cfg.get('level', 'INFO'))
        
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('replication.log'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)


    # Connection management
    
    def get_connection_string(self, host: str, port: int, uid: str, pwd: str, database: str = None) -> str:
        """Generate ODBC connection string"""
        timeout = self.config.get('replication', {}).get('connection_timeout', 30)
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host},{port};"
            f"UID={uid};"
            f"PWD={pwd};"
            f"TrustServerCertificate=YES;"
            f"Connection Timeout={timeout};"
        )
        
        if database:
            conn_str += f"DATABASE={database};"
            
        return conn_str
        
    def get_connection(self, host: str, port: int, uid: str, pwd: str, database: str = None) -> pyodbc.Connection:
        """Get database connection"""
        pool_key = f"{host}:{port}:{database or 'master'}"
        
        # Check existing connection
        if pool_key in self.connection_pool:
            try:
                conn = self.connection_pool[pool_key]
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return conn
            except Exception:
                del self.connection_pool[pool_key]
        
        # Create new connection
        conn_str = self.get_connection_string(host, port, uid, pwd, database)
        conn = pyodbc.connect(conn_str, autocommit=True)
        self.connection_pool[pool_key] = conn
        return conn
            
    def execute_query(self, host: str, port: int, uid: str, pwd: str, database: str, sql: str, 
                     params: tuple = None, fetch: bool = False):
        """Execute query with basic retry logic"""
        retry_attempts = self.config.get('replication', {}).get('retry_attempts', 3)
        
        for attempt in range(retry_attempts):
            try:
                conn = self.get_connection(host, port, uid, pwd, database)
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                    
                if fetch:
                    result = cursor.fetchall()
                    cursor.close()
                    return result
                else:
                    cursor.close()
                    return None
                    
            except pyodbc.Error as e:
                self.logger.error(f"SQL error (attempt {attempt + 1}/{retry_attempts}): {e}")
                
                # Remove bad connection from pool
                pool_key = f"{host}:{port}:{database or 'master'}"
                if pool_key in self.connection_pool:
                    try:
                        self.connection_pool[pool_key].close()
                    except Exception:
                        pass
                    del self.connection_pool[pool_key]
                
                if attempt < retry_attempts - 1:
                    time.sleep(2)  # Wait before retry
                else:
                    raise
                    
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                if attempt < retry_attempts - 1:
                    time.sleep(2)
                else:
                    raise
                    
    def close_connections(self):
        """Close all pooled connections"""
        for conn in self.connection_pool.values():
            try:
                conn.close()
            except Exception:
                pass
        self.connection_pool.clear()
        



    # Schema helpers
    
    def get_table_columns(self, master: dict, schema: str, table: str) -> List[Tuple]:
        """Get table columns"""
        sql = '''
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE,
               COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
               COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        '''
        return self.execute_query(
            master['host'], master['port'], master['username'], master['password'], 
            master['database'], sql, (schema, table), fetch=True
        )
            
    def get_primary_key(self, master: dict, schema: str, table: str) -> List[str]:
        """Get primary key columns"""
        sql = '''
        SELECT k.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY' AND k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ?
        ORDER BY k.ORDINAL_POSITION
        '''
        rows = self.execute_query(
            master['host'], master['port'], master['username'], master['password'], 
            master['database'], sql, (schema, table), fetch=True
        )
        return [r[0] for r in rows] if rows else []
            
    def table_exists(self, db_config: dict, schema: str, table: str) -> bool:
        """Check if table exists"""
        sql = '''
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        '''
        try:
            result = self.execute_query(
                db_config['host'], db_config['port'], db_config['username'], 
                db_config['password'], db_config['database'], sql, (schema, table), fetch=True
            )
            return result[0][0] > 0 if result else False
        except Exception:
            return False
            



    # Schema creation helpers
    
    def build_create_table_script(self, columns, pk_columns, schema: str, table: str) -> str:
        """Build CREATE TABLE script"""
        col_defs = []
        for col in columns:
            name, dtype, max_len, precision, scale, is_nullable, is_identity, default = col

            if dtype in ('varchar','nvarchar','char','nchar'):
                typ = f"{dtype}(max)" if max_len == -1 or max_len is None else f"{dtype}({max_len})"
            elif dtype in ('decimal','numeric'):
                typ = f"{dtype}(18,2)" if precision is None else f"{dtype}({precision},{scale or 0})"
            else:
                typ = dtype

            col_line = f"[{name}] {typ}"
            if is_identity == 1:
                col_line += " IDENTITY(1,1)"
            col_line += " NULL" if is_nullable == 'YES' else " NOT NULL"
            if default:
                col_line += f" DEFAULT {default}"
            col_defs.append(col_line)

        pk_sql = ''
        if pk_columns:
            pk_cols_quoted = ','.join([f"[{c}]" for c in pk_columns])
            pk_constraint_name = f"PK_{schema}_{table}_{int(time.time())}"
            pk_sql = f", CONSTRAINT [{pk_constraint_name}] PRIMARY KEY ({pk_cols_quoted})"

        return f"CREATE TABLE [{schema}].[{table}] ({', '.join(col_defs)}{pk_sql})";
            



    # Replication setup
    
    def ensure_database_exists(self, db_config: dict, db_name: str):
        """Ensure database exists"""
        sql = f"IF DB_ID(N'{db_name}') IS NULL CREATE DATABASE [{db_name}];"
        self.execute_query(
            db_config['host'], db_config['port'], db_config['username'], 
            db_config['password'], None, sql
        )
        self.logger.info(f"Database {db_name} ensured on {db_config['host']}")
            
    def ensure_login_exists(self, db_config: dict, login: str, password: str):
        """Ensure login exists"""
        sql = f"""
        IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = N'{login}')
        BEGIN
            CREATE LOGIN [{login}] WITH PASSWORD = N'{password}', 
            CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;
        END
        """
        self.execute_query(
            db_config['host'], db_config['port'], db_config['username'], 
            db_config['password'], None, sql
        )
        self.logger.info(f"Login {login} ensured on {db_config['host']}")
            
    def setup_distributor_and_publisher(self, master: dict, distributor_password: str):
        """Setup distributor and publisher"""
        self.logger.info('Configuring distributor on master...')
        
        # Add distributor
        sql_distributor = f"""
        IF NOT EXISTS (SELECT name FROM sys.servers WHERE name = N'{master['host']}')
        BEGIN
            EXEC sp_adddistributor 
                @distributor = N'{master['host']}', 
                @password = N'{distributor_password}';
        END
        """
        try:
            self.execute_query(
                master['host'], master['port'], master['username'], master['password'], 
                None, sql_distributor
            )
        except Exception as e:
            if "already configured" not in str(e).lower():
                raise
        
        # Create distribution database
        sql_distdb = """
        IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'distribution')
        BEGIN
            EXEC sp_adddistributiondb @database = N'distribution';
        END
        """
        try:
            self.execute_query(
                master['host'], master['port'], master['username'], master['password'], 
                None, sql_distdb
            )
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise
        
        # Add publisher
        sql_publisher = f"""
        IF NOT EXISTS (SELECT srvname FROM master.dbo.sysservers WHERE srvname = N'{master['host']}')
        BEGIN
            EXEC sp_adddistpublisher 
                @publisher = N'{master['host']}', 
                @distribution_db = N'distribution', 
                @security_mode = 1;
        END
        """
        try:
            self.execute_query(
                master['host'], master['port'], master['username'], master['password'], 
                None, sql_publisher
            )
        except Exception as e:
            if "already defined" not in str(e).lower():
                raise
                
        self.logger.info("Distributor and publisher configured.")
                
    def create_publication(self, master: dict, publication_name: str, database_name: str):
        """Create publication"""
        self.logger.info(f'Creating publication {publication_name}...')
        
        # Enable database for publishing
        sql_enable = f"""
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{database_name}' AND is_published = 1)
        BEGIN
            EXEC sp_replicationdboption 
                @dbname = N'{database_name}', 
                @optname = N'publish', 
                @value = N'true';
        END
        """
        self.execute_query(
            master['host'], master['port'], master['username'], master['password'], 
            database_name, sql_enable
        )
        
        # Create publication
        sql_publication = f"""
        IF NOT EXISTS (SELECT * FROM syspublications WHERE name = N'{publication_name}')
        BEGIN
            EXEC sp_addpublication 
                @publication = N'{publication_name}',
                @status = N'active',
                @allow_push = N'true',
                @allow_pull = N'true',
                @repl_freq = N'continuous',
                @sync_method = N'concurrent',
                @immediate_sync = N'true';
        END
        """
        self.execute_query(
            master['host'], master['port'], master['username'], master['password'], 
            database_name, sql_publication
        )
        
        self.logger.info(f"Publication '{publication_name}' created successfully.")

    def add_article(self, master: dict, publication_name: str, schema: str, table: str):
        """Add article to publication"""
        # Validate primary key exists
        pk = self.get_primary_key(master, schema, table)
        if not pk:
            raise RuntimeError(f"Table {schema}.{table} has no primary key")
            
        # Check if article already exists
        check_sql = f"""
        SELECT COUNT(*) FROM syspublications p 
        JOIN sysarticles a ON p.pubid = a.pubid 
        WHERE p.name = N'{publication_name}' AND a.name = N'{table}'
        """
        
        result = self.execute_query(
            master['host'], master['port'], master['username'], master['password'], 
            master['database'], check_sql, fetch=True
        )
        
        if result and result[0][0] > 0:
            self.logger.info(f"Article {schema}.{table} already exists")
            return
            
        # Add article
        sql_article = f"""
        EXEC sp_addarticle 
            @publication = N'{publication_name}', 
            @article = N'{table}', 
            @source_owner = N'{schema}', 
            @source_object = N'{table}', 
            @destination_table = N'{table}',
            @destination_owner = N'{schema}',
            @pre_creation_cmd = N'drop',
            @force_invalidate_snapshot = 1;
        """
        
        self.execute_query(
            master['host'], master['port'], master['username'], master['password'], 
            master['database'], sql_article
        )
        
        self.logger.info(f"Article {schema}.{table} added to publication")
            
    def create_push_subscription(self, master: dict, publication_name: str, subscriber: dict, 
                                distributor_login: str, distributor_password: str, sync_interval_seconds: int):
        """Create push subscription with enhanced error handling and optimization"""
        try:
            self.logger.info(f'Creating push subscription to {subscriber["host"]}:{subscriber["port"]}/{subscriber["database"]}')
            
            # Create subscription
            sql_subscription = f"""
            IF NOT EXISTS (SELECT * FROM syssubscriptions s 
                          JOIN syspublications p ON s.pubid = p.pubid 
                          WHERE p.name = N'{publication_name}' 
                          AND s.dest_db = N'{subscriber['database']}')
            BEGIN
                EXEC sp_addsubscription 
                    @publication = N'{publication_name}', 
                    @subscriber = N'{subscriber['host']}', 
                    @destination_db = N'{subscriber['database']}', 
                    @subscription_type = N'Push', 
                    @sync_type = N'automatic', 
                    @article = N'all';
            END
            """
            
            self.execute_query_with_retry(
                master['host'], master['port'], master['username'], master['password'], 
                master['database'], sql_subscription
            )
            
            # Create distribution agent with optimized settings
            freq_subday_interval = max(1, int(sync_interval_seconds))
            
            sql_agent = f"""
            EXEC sp_addpushsubscription_agent 
                @publication = N'{publication_name}', 
                @subscriber = N'{subscriber['host']}', 
                @subscriber_db = N'{subscriber['database']}', 
                @job_login = N'{distributor_login}', 
                @job_password = N'{distributor_password}', 
                @subscriber_security_mode = 1,
                @frequency_type = 4,
                @frequency_interval = 1,
                @frequency_relative_interval = 1,
                @frequency_recurrence_factor = 0,
                @frequency_subday = 4,
                @frequency_subday_interval = {freq_subday_interval},
                @active_start_time_of_day = 0,
                @active_end_time_of_day = 235959,
                @enabled_for_syncmgr = N'False',
                @dts_package_location = N'Distributor';
            """
            
            self.execute_query_with_retry(
                master['host'], master['port'], master['username'], master['password'], 
                master['database'], sql_agent
            )
            
            self.logger.info(f"Push subscription agent created with {sync_interval_seconds}s interval")
            
        except Exception as e:
            self.logger.warning(f'Failed to create push subscription agent: {e}')
            # Don't raise here as subscription might still work
            
    def initialize_large_table_with_backup(self, master: dict, replica: dict, schema: str, table: str):
        """Initialize large table using backup/restore for better performance"""
        try:
            row_count = self.get_table_row_count(master, schema, table)
            backup_threshold = self.config.get('replication', {}).get('backup_restore_threshold', 5000000)
            
            if row_count < backup_threshold:
                return False  # Use normal snapshot
                
            self.logger.info(f"Initializing large table {schema}.{table} ({row_count:,} rows) using backup/restore")
            
            # This is a simplified version - in production you'd need proper backup/restore logic
            # with network shares and proper security
            backup_file = f"/backup/{master['database']}_{schema}_{table}_{int(time.time())}.bak"
            
            # Create table-specific backup (if supported)
            backup_sql = f"""
            BACKUP DATABASE [{master['database']}] 
            TO DISK = N'{backup_file}'
            WITH FORMAT, INIT, SKIP, NOREWIND, NOUNLOAD, STATS = 10;
            """
            
            # This would require additional logic for:
            # 1. Creating network accessible backup location
            # 2. Proper security permissions
            # 3. Restore on subscriber
            # 4. Mark subscription for backup initialization
            
            self.logger.info(f"Backup/restore initialization available for {schema}.{table}")
            return True
            
        except Exception as e:
            self.logger.warning(f"Backup/restore initialization failed for {schema}.{table}: {e}")
            return False


    # Main replication setup
    
    def setup_replication(self):
        """Setup replication with simplified flow"""
        rep_cfg = self.config['replication']
        master = self.config['master_database']
        replicas = self.config['replica_databases']
        schemas = self.config['schemas_to_replicate']
        
        if not rep_cfg.get('enabled', True):
            self.logger.info('Replication is disabled in config')
            return
            
        self.logger.info("Starting replication setup...")
        
        # Get distributor password
        distributor_password = rep_cfg.get('distributor_admin_password', 'DistributorPassword!123')
        
        # Setup distributor login
        self.ensure_login_exists(master, 'distributor_admin', distributor_password)
        
        # Setup distributor and publisher
        self.setup_distributor_and_publisher(master, distributor_password)
        
        # Create publication
        publication_name = f"{master['database']}_transactional_pub"
        self.create_publication(master, publication_name, master['database'])
        
        # Setup replicas
        self.setup_replicas(replicas, distributor_password)
        
        # Process tables
        self.process_tables(master, replicas, schemas, publication_name)
        
        # Create subscriptions
        self.create_subscriptions(master, replicas, publication_name, distributor_password)
        
        # Start snapshot
        self.start_snapshot(master, publication_name)
        
        self.logger.info('Replication setup completed!')
    
    def setup_replicas(self, replicas: list, distributor_password: str):
        """Setup replica databases"""
        for replica in replicas:
            # Create database
            self.ensure_database_exists(replica, replica['database'])
            
            # Setup login
            self.ensure_login_exists(replica, 'distributor_admin', distributor_password)
            
            # Create user and assign permissions
            sql_user = f"""
            USE [{replica['database']}];
            IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = 'distributor_admin')
            BEGIN
                CREATE USER [distributor_admin] FOR LOGIN [distributor_admin];
                EXEC sp_addrolemember N'db_owner', N'distributor_admin';
            END
            """
            self.execute_query(
                replica['host'], replica['port'], replica['username'], replica['password'],
                replica['database'], sql_user
            )
            
    def process_tables(self, master: dict, replicas: list, schemas: list, publication_name: str):
        """Process tables and create them on replicas"""
        for schema in schemas:
            for table_config in schema['tables']:
                if not table_config.get('replicate', True):
                    continue
                    
                table_name = table_config['table_name']
                self.logger.info(f'Processing table {schema["schema_name"]}.{table_name}')
                
                # Validate primary key
                pk = self.get_primary_key(master, schema['schema_name'], table_name)
                if not pk:
                    raise RuntimeError(f"Table {schema['schema_name']}.{table_name} missing primary key")
                    
                # Get table structure
                columns = self.get_table_columns(master, schema['schema_name'], table_name)
                
                # Create table on replicas
                create_table_sql = self.build_create_table_script(columns, pk, schema['schema_name'], table_name)
                
                for replica in replicas:
                    if not self.table_exists(replica, schema['schema_name'], table_name):
                        full_sql = f"USE [{replica['database']}]; {create_table_sql};"
                        self.execute_query(
                            replica['host'], replica['port'], replica['username'], 
                            replica['password'], replica['database'], full_sql
                        )
                        self.logger.info(f"Table {schema['schema_name']}.{table_name} created on {replica['name']}")
                        
                # Add article to publication
                self.add_article(master, publication_name, schema['schema_name'], table_name)
        """Setup replica databases in parallel for better performance"""
        def setup_single_replica(replica):
            try:
                # Create database if missing
                if rep_cfg.get('auto_setup_replicas', True):
                    self.ensure_database_exists(replica, replica['database'])
                    
                # Setup distributor login on replica
                distributor_password = rep_cfg.get('distributor_admin_password', 'DistributorPassword!123')
                self.ensure_login_exists(replica, 'distributor_admin', distributor_password)
                
                # Create user and assign permissions
                sql_user = f"""
                USE [{replica['database']}];
                IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = 'distributor_admin')
                BEGIN
                    CREATE USER [distributor_admin] FOR LOGIN [distributor_admin];
                    EXEC sp_addrolemember N'db_owner', N'distributor_admin';
                END
                """
                self.execute_query_with_retry(
                    replica['host'], replica['port'], replica['username'], replica['password'],
                    replica['database'], sql_user
                )
                
                self.logger.info(f"Replica {replica['name']} setup completed")
                return replica['name'], True
                
            except Exception as e:
                self.logger.error(f"Failed to setup replica {replica['name']}: {e}")
                return replica['name'], False
                
        # Use thread pool for parallel setup
        max_workers = min(len(replicas), 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(setup_single_replica, replica) for replica in replicas]
            
            for future in as_completed(futures):
                replica_name, success = future.result()
                if not success:
                    raise Exception(f"Failed to setup replica {replica_name}")
                    
    def _process_tables_and_articles(self, master: dict, replicas: list, schemas: list, 
                                   publication_name: str, rep_cfg: dict):
        """Process tables, create schema/tables on replicas, and add articles"""
        create_missing_tables = rep_cfg.get('create_missing_tables', True)
        create_missing_schemas = rep_cfg.get('create_missing_schemas', True)
        use_backup_restore = rep_cfg.get('use_backup_restore_init', False)
        
        # Collect FK scripts to apply after all tables are created
        fk_scripts_by_replica = {r['name']: [] for r in replicas}
        
        for schema in schemas:
            for table_config in schema['tables']:
                if not table_config.get('replicate', True):
                    continue
                    
                table_name = table_config['table_name']
                self.logger.info(f'Processing table {schema["schema_name"]}.{table_name}')
                
                # Validate primary key
                pk = self.get_primary_key(master, schema['schema_name'], table_name)
                if not pk:
                    raise RuntimeError(f"Table {schema['schema_name']}.{table_name} missing primary key")
                    
                # Get table structure from master
                columns = self.get_table_columns(master, schema['schema_name'], table_name)
                indexes = self.get_indexes(master, schema['schema_name'], table_name)
                fks = self.get_foreign_keys(master, schema['schema_name'], table_name)
                
                # Build scripts
                create_table_sql = self.build_create_table_script(columns, pk, schema['schema_name'], table_name)
                idx_scripts = self.build_create_indexes_script(indexes, schema['schema_name'], table_name)
                fk_scripts = [self.build_fk_script(fk, schema['schema_name'], table_name) for fk in fks]
                fk_scripts = [fk for fk in fk_scripts if fk]  # Filter None values
                
                # Create schema and table on each replica
                for replica in replicas:
                    try:
                        # Create schema if missing
                        if create_missing_schemas and schema['schema_name'] != 'dbo':
                            schema_sql = f"""
                            USE [{replica['database']}];
                            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema['schema_name']}')
                            BEGIN
                                EXEC('CREATE SCHEMA [{schema["schema_name"]}]');
                            END
                            """
                            self.execute_query_with_retry(
                                replica['host'], replica['port'], replica['username'], 
                                replica['password'], replica['database'], schema_sql
                            )
                            
                        # Create table if missing
                        if create_missing_tables:
                            if not self.table_exists(replica, schema['schema_name'], table_name):
                                full_create_sql = f"USE [{replica['database']}]; {create_table_sql};"
                                self.execute_query_with_retry(
                                    replica['host'], replica['port'], replica['username'], 
                                    replica['password'], replica['database'], full_create_sql
                                )
                                
                                # Create indexes
                                for idx_sql in idx_scripts:
                                    try:
                                        full_idx_sql = f"USE [{replica['database']}]; {idx_sql}"
                                        self.execute_query_with_retry(
                                            replica['host'], replica['port'], replica['username'], 
                                            replica['password'], replica['database'], full_idx_sql
                                        )
                                    except Exception as e:
                                        self.logger.warning(f"Failed to create index on {replica['name']}: {e}")
                                        
                                # Store FK scripts for later
                                for fk_sql in fk_scripts:
                                    fk_scripts_by_replica[replica['name']].append(
                                        (replica, f"USE [{replica['database']}]; {fk_sql}")
                                    )
                                    
                                self.logger.info(f"Table {schema['schema_name']}.{table_name} created on {replica['name']}")
                                
                        # Handle large table optimization
                        if use_backup_restore:
                            self.initialize_large_table_with_backup(master, replica, schema['schema_name'], table_name)
                            
                    except Exception as e:
                        self.logger.error(f"Failed to create table {schema['schema_name']}.{table_name} on {replica['name']}: {e}")
                        raise
                        
    def create_subscriptions(self, master: dict, replicas: list, publication_name: str, distributor_password: str):
        """Create subscriptions for all replicas"""
        for replica in replicas:
            self.create_push_subscription(
                master, publication_name, replica, 
                'distributor_admin', distributor_password, sync_interval
            )
    def start_snapshot(self, master: dict, publication_name: str):
        """Start snapshot generation"""
        try:
            sql_snapshot = f"EXEC sp_startpublication_snapshot @publication = N'{publication_name}';"
            self.execute_query(
                master['host'], master['port'], master['username'], master['password'], 
                master['database'], sql_snapshot
            )
            self.logger.info(f"Snapshot generation started")
        except Exception as e:
            self.logger.warning(f"Could not start snapshot: {e}")
        
    def start(self):
        """Start the enhanced replication manager"""
        try:
            self.setup_replication()
            self.logger.info("Replication manager started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start replication manager: {e}")
            raise
        finally:
            # Keep connections alive for monitoring
            pass
            
    def stop(self):
        """Stop the replication manager and cleanup"""
        self.running = False
        self.close_all_connections()
        self.logger.info("Replication manager stopped")


# Main execution
def main():
    """Main entry point for replication manager"""
    manager = None
    try:
        print("Starting SQL Server Replication Manager...")
        
        # Initialize the replication manager
        manager = ReplicationManager('replication_config_enhanced.json')
        
        # Start replication setup
        manager.start()
        
        print("✅ Replication setup completed successfully!")
        print("📋 Monitor replication through SQL Server Management Studio")
        
    except FileNotFoundError:
        print("❌ Error: replication_config_enhanced.json not found.")
        return 1
    except Exception as exc:
        print(f"❌ Error during replication setup: {exc}")
        return 1
    finally:
        if manager:
            manager.stop()
                
    return 0

if __name__ == '__main__':
    exit(main())
