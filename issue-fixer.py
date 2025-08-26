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
        print(f"✅ SQL command executed successfully")
        return True
    except pyodbc.Error as ex:
        print(f"❌ SQL Error: {ex}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_config(config_path='replication_config_enhanced.json'):
    """Loads the replication configuration from a JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

def fix_replication_complete(config):
    """Complete replication fix procedure."""
    master_config = config['master_database']
    replicas_config = config['replica_databases']
    schemas_config = config['schemas_to_replicate']
    replication_config = config['replication']
    
    distributor_password = replication_config.get('distributor_admin_password')
    master_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={master_config['host']},{master_config['port']};UID={master_config['username']};PWD={master_config['password']};TrustServerCertificate=yes"
    master_conn_str_db = f"{master_conn_str};DATABASE={master_config['database']}"
    
    print("=" * 60)
    print("COMPREHENSIVE REPLICATION FIX PROCEDURE")
    print("=" * 60)
    
    # Step 1: Enable SQL Server Agent (if not already running)
    print("\n1. ENABLING SQL SERVER AGENT")
    print("-" * 30)
    agent_sql = """
    EXEC xp_servicecontrol 'start', 'SQLServerAGENT'
    """
    # Note: This might not work in all environments, especially containers
    print("⚠️ Note: SQL Server Agent should be enabled in your Docker containers")
    print("   Add MSSQL_AGENT_ENABLED=true to your docker-compose.yml (already present)")
    
    # Step 2: Clean up existing broken replication
    print("\n2. CLEANING UP EXISTING REPLICATION")
    print("-" * 30)
    
    # Remove existing subscriptions
    cleanup_subscriptions = """
    DECLARE @publication NVARCHAR(128) = 'AskdTransactionalPublication'
    
    -- Remove push subscriptions
    DECLARE subscription_cursor CURSOR FOR
    SELECT subscriber_server, subscriber_db
    FROM syssubscriptions s
    INNER JOIN syspublications p ON s.pubid = p.pubid
    WHERE p.name = @publication
    
    DECLARE @subscriber NVARCHAR(128), @subscriber_db NVARCHAR(128)
    OPEN subscription_cursor
    FETCH NEXT FROM subscription_cursor INTO @subscriber, @subscriber_db
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC sp_dropsubscription 
            @publication = @publication,
            @subscriber = @subscriber,
            @destination_db = @subscriber_db,
            @article = 'all'
        
        FETCH NEXT FROM subscription_cursor INTO @subscriber, @subscriber_db
    END
    
    CLOSE subscription_cursor
    DEALLOCATE subscription_cursor
    """
    execute_sql(master_conn_str_db, cleanup_subscriptions)
    
    # Remove existing publication
    drop_publication = """
    IF EXISTS (SELECT name FROM syspublications WHERE name = 'AskdTransactionalPublication')
    BEGIN
        EXEC sp_droppublication @publication = N'AskdTransactionalPublication'
    END
    """
    execute_sql(master_conn_str_db, drop_publication)
    
    # Step 3: Recreate publication with proper settings
    print("\n3. RECREATING PUBLICATION")
    print("-" * 30)
    
    create_publication = f"""
    USE {master_config['database']}
    
    -- Create publication
    EXEC sp_addpublication 
        @publication = N'AskdTransactionalPublication',
        @description = N'Transactional publication of askd database.',
        @sync_method = N'concurrent',
        @repl_freq = N'continuous',
        @status = N'active',
        @allow_push = N'true',
        @allow_pull = N'false',
        @immediate_sync = N'false',
        @allow_sync_tran = N'false',
        @autogen_sync_procs = N'false',
        @retention = 336
    """
    if not execute_sql(master_conn_str_db, create_publication):
        print("❌ Failed to create publication")
        return False
    
    # Step 4: Add articles with proper settings
    print("\n4. ADDING ARTICLES TO PUBLICATION")
    print("-" * 30)
    
    for schema in schemas_config:
        for table in schema['tables']:
            add_article = f"""
            USE {master_config['database']}
            
            EXEC sp_addarticle 
                @publication = N'AskdTransactionalPublication',
                @article = N'{table['table_name']}',
                @source_object = N'{table['table_name']}',
                @source_owner = N'{schema['schema_name']}',
                @destination_table = N'{table['table_name']}',
                @destination_owner = N'{schema['schema_name']}',
                @type = N'logbased',
                @description = N'Article for table {table['table_name']}',
                @creation_script = NULL,
                @pre_creation_cmd = N'drop',
                @schema_option = 0x000000000803509F,
                @identityrangemanagementoption = N'manual',
                @status = 24,
                @ins_cmd = N'CALL sp_MSins_{table['table_name']}',
                @del_cmd = N'CALL sp_MSdel_{table['table_name']}',
                @upd_cmd = N'SCALL sp_MSupd_{table['table_name']}'
            """
            if execute_sql(master_conn_str_db, add_article):
                print(f"✅ Added article for {schema['schema_name']}.{table['table_name']}")
            else:
                print(f"❌ Failed to add article for {schema['schema_name']}.{table['table_name']}")
    
    # Step 5: Create initial snapshot
    print("\n5. CREATING INITIAL SNAPSHOT")
    print("-" * 30)
    
    create_snapshot = f"""
    USE {master_config['database']}
    EXEC sp_startpublication_snapshot @publication = N'AskdTransactionalPublication'
    """
    if execute_sql(master_conn_str_db, create_snapshot):
        print("✅ Snapshot creation started")
        print("   Waiting for snapshot to complete...")
        
        # Wait for snapshot to complete
        for i in range(20):  # Wait up to 10 minutes
            time.sleep(30)
            
            # Check snapshot status
            check_snapshot = """
            SELECT 
                status,
                warning,
                last_distsync
            FROM distribution.dbo.MSsnapshot_agents
            WHERE publication = 'AskdTransactionalPublication'
            """
            conn = pyodbc.connect(master_conn_str)
            cursor = conn.cursor()
            cursor.execute(check_snapshot)
            result = cursor.fetchone()
            conn.close()
            
            if result:
                status = result[0]
                if status == 2:  # Success
                    print("✅ Snapshot completed successfully")
                    break
                elif status == 6:  # Fail
                    print("❌ Snapshot failed")
                    return False
                else:
                    print(f"   Snapshot in progress... (attempt {i+1}/20)")
            else:
                print(f"   Checking snapshot status... (attempt {i+1}/20)")
        else:
            print("⚠️ Snapshot is taking longer than expected, continuing...")
    
    # Step 6: Add subscriptions with corrected settings
    print("\n6. ADDING SUBSCRIPTIONS")
    print("-" * 30)
    
    for replica in replicas_config:
        add_subscription = f"""
        USE {master_config['database']}
        
        -- Add subscription
        EXEC sp_addsubscription 
            @publication = N'AskdTransactionalPublication',
            @subscriber = N'{replica['host']},{replica['port']}',
            @destination_db = N'{replica['database']}',
            @subscription_type = N'Push',
            @sync_type = N'automatic',
            @article = N'all',
            @update_mode = N'read only',
            @subscriber_type = 0
        
        -- Add push subscription agent
        EXEC sp_addpushsubscription_agent 
            @publication = N'AskdTransactionalPublication',
            @subscriber = N'{replica['host']},{replica['port']}',
            @subscriber_db = N'{replica['database']}',
            @job_login = N'distributor_admin',
            @job_password = N'{distributor_password}',
            @subscriber_security_mode = 0,
            @subscriber_login = N'SA',
            @subscriber_password = N'{replica['password']}',
            @distributor_security_mode = 0,
            @distributor_login = N'distributor_admin',
            @distributor_password = N'{distributor_password}',
            @frequency_type = 4,
            @frequency_interval = 1,
            @frequency_relative_interval = 1,
            @frequency_recurrence_factor = 0,
            @frequency_subday = 4,
            @frequency_subday_interval = 15,
            @active_start_time_of_day = 0,
            @active_end_time_of_day = 235959,
            @active_start_date = 0,
            @active_end_date = 0
        """
        if execute_sql(master_conn_str_db, add_subscription):
            print(f"✅ Added subscription for {replica['name']}")
        else:
            print(f"❌ Failed to add subscription for {replica['name']}")
    
    # Step 7: Start replication agents
    print("\n7. STARTING REPLICATION AGENTS")
    print("-" * 30)
    
    # Start Log Reader Agent
    start_logreader = """
    DECLARE @job_name NVARCHAR(128)
    SELECT TOP 1 @job_name = name 
    FROM msdb.dbo.sysjobs 
    WHERE name LIKE '%LogReader%'
    
    IF @job_name IS NOT NULL
    BEGIN
        EXEC msdb.dbo.sp_start_job @job_name = @job_name
    END
    """
    execute_sql(master_conn_str, start_logreader)
    
    # Start Distribution Agents
    start_distribution = """
    DECLARE agent_cursor CURSOR FOR
    SELECT name 
    FROM msdb.dbo.sysjobs 
    WHERE name LIKE '%distribution%' OR name LIKE '%AskdTransactionalPublication%'
    
    DECLARE @agent_name NVARCHAR(128)
    OPEN agent_cursor
    FETCH NEXT FROM agent_cursor INTO @agent_name
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC msdb.dbo.sp_start_job @job_name = @agent_name
        FETCH NEXT FROM agent_cursor INTO @agent_name
    END
    
    CLOSE agent_cursor
    DEALLOCATE agent_cursor
    """
    execute_sql(master_conn_str, start_distribution)
    
    print("\n" + "=" * 60)
    print("REPLICATION FIX PROCEDURE COMPLETED")
    print("=" * 60)
    print("⏳ Waiting 60 seconds for replication to initialize...")
    time.sleep(60)
    
    return True

if __name__ == "__main__":
    try:
        config = load_config()
        if fix_replication_complete(config):
            print("\n🎉 Replication fix completed successfully!")
            print("\nRunning verification to check results...")
            
            # Run verification
            import subprocess
            subprocess.run(["python", "verify_replication.py"])
        else:
            print("\n❌ Replication fix failed. Please check the error messages above.")
            
    except FileNotFoundError:
        print("Error: replication_config_enhanced.json not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")



