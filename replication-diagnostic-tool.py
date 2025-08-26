import pyodbc
import json
import time
from datetime import datetime

def load_config(config_path='replication_config_enhanced.json'):
    """Loads the replication configuration from a JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

def execute_query(conn_str, query, fetch_all=False):
    """Execute a query and return results."""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)
        if fetch_all:
            return cursor.fetchall()
        else:
            return cursor.fetchone()
    except pyodbc.Error as ex:
        print(f"SQL Error: {ex}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def diagnose_replication(config):
    """Comprehensive replication diagnostics."""
    master_config = config['master_database']
    replicas_config = config['replica_databases']
    
    master_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={master_config['host']},{master_config['port']};UID={master_config['username']};PWD={master_config['password']};TrustServerCertificate=yes"
    master_conn_str_db = f"{master_conn_str};DATABASE={master_config['database']}"
    
    print("=" * 60)
    print("SQL SERVER REPLICATION DIAGNOSTIC REPORT")
    print("=" * 60)
    print(f"Generated at: {datetime.now()}")
    print()
    
    # 1. Check if SQL Server Agent is running
    print("1. SQL SERVER AGENT STATUS")
    print("-" * 30)
    agent_query = """
    SELECT 
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.dm_server_services WHERE servicename LIKE '%SQL Server Agent%' AND status = 4)
            THEN 'RUNNING ✅'
            ELSE 'NOT RUNNING ❌'
        END as AgentStatus,
        servicename,
        status_desc
    FROM sys.dm_server_services 
    WHERE servicename LIKE '%SQL Server Agent%'
    """
    result = execute_query(master_conn_str, agent_query, fetch_all=True)
    if result:
        for row in result:
            print(f"Service: {row[1]}")
            print(f"Status: {row[0]} - {row[2]}")
    else:
        print("❌ Could not check SQL Server Agent status")
    print()
    
    # 2. Check distributor configuration
    print("2. DISTRIBUTOR CONFIGURATION")
    print("-" * 30)
    distributor_query = """
    SELECT 
        name as distributor_name,
        data_folder,
        log_folder,
        min_distretention,
        max_distretention
    FROM distribution.dbo.MSdistributor
    """
    result = execute_query(master_conn_str, distributor_query)
    if result:
        print(f"✅ Distributor configured: {result[0]}")
        print(f"Data folder: {result[1]}")
        print(f"Log folder: {result[2]}")
    else:
        print("❌ Distributor not configured properly")
    print()
    
    # 3. Check publication status
    print("3. PUBLICATION STATUS")
    print("-" * 30)
    pub_query = """
    SELECT 
        name,
        status,
        sync_method,
        repl_freq,
        description
    FROM syspublications
    """
    result = execute_query(master_conn_str_db, pub_query, fetch_all=True)
    if result:
        for row in result:
            status_text = "ACTIVE ✅" if row[1] == 1 else "INACTIVE ❌"
            print(f"Publication: {row[0]}")
            print(f"Status: {status_text}")
            print(f"Sync Method: {row[2]}")
            print(f"Replication Frequency: {row[3]}")
            print()
    else:
        print("❌ No publications found")
    print()
    
    # 4. Check articles (tables) in publication
    print("4. PUBLISHED ARTICLES")
    print("-" * 30)
    articles_query = """
    SELECT 
        a.name as article_name,
        a.source_owner,
        a.source_object,
        a.destination_object,
        a.status,
        p.name as publication_name
    FROM sysarticles a
    INNER JOIN syspublications p ON a.pubid = p.pubid
    """
    result = execute_query(master_conn_str_db, articles_query, fetch_all=True)
    if result:
        for row in result:
            status_text = "ACTIVE ✅" if row[4] & 1 else "INACTIVE ❌"
            print(f"Article: {row[0]}")
            print(f"Source: [{row[1]}].[{row[2]}]")
            print(f"Status: {status_text}")
            print(f"Publication: {row[5]}")
            print()
    else:
        print("❌ No articles found in publications")
    print()
    
    # 5. Check subscriptions
    print("5. SUBSCRIPTION STATUS")
    print("-" * 30)
    subscription_query = """
    SELECT 
        s.subscriber_server,
        s.subscriber_db,
        s.subscription_type,
        s.status,
        s.sync_type,
        p.name as publication_name
    FROM syssubscriptions s
    INNER JOIN syspublications p ON s.pubid = p.pubid
    """
    result = execute_query(master_conn_str_db, subscription_query, fetch_all=True)
    if result:
        for row in result:
            status_text = "ACTIVE ✅" if row[3] == 1 else "INACTIVE ❌"
            sub_type = "PUSH" if row[2] == 0 else "PULL"
            print(f"Subscriber: {row[0]}")
            print(f"Database: {row[1]}")
            print(f"Type: {sub_type}")
            print(f"Status: {status_text}")
            print(f"Sync Type: {row[4]}")
            print(f"Publication: {row[5]}")
            print()
    else:
        print("❌ No subscriptions found")
    print()
    
    # 6. Check replication agents
    print("6. REPLICATION AGENTS")
    print("-" * 30)
    agents_query = """
    SELECT 
        j.name as job_name,
        j.enabled,
        ja.last_run_date,
        ja.last_run_time,
        ja.last_run_outcome,
        ja.last_run_duration
    FROM msdb.dbo.sysjobs j
    INNER JOIN msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id
    WHERE j.name LIKE '%repl%' OR j.name LIKE '%distribution%'
    """
    result = execute_query(master_conn_str, agents_query, fetch_all=True)
    if result:
        for row in result:
            enabled_text = "ENABLED ✅" if row[1] == 1 else "DISABLED ❌"
            outcome_text = "SUCCESS ✅" if row[4] == 1 else "FAILED ❌" if row[4] == 0 else "UNKNOWN"
            print(f"Job: {row[0]}")
            print(f"Enabled: {enabled_text}")
            print(f"Last Run Date: {row[2]}")
            print(f"Last Run Outcome: {outcome_text}")
            print()
    else:
        print("❌ No replication agent jobs found")
    print()
    
    # 7. Check distribution database errors
    print("7. RECENT REPLICATION ERRORS")
    print("-" * 30)
    error_query = """
    SELECT TOP 10
        time,
        error_id,
        error_text,
        source_type_desc
    FROM distribution.dbo.MSrepl_errors
    ORDER BY time DESC
    """
    result = execute_query(master_conn_str, error_query, fetch_all=True)
    if result:
        for row in result:
            print(f"Time: {row[0]}")
            print(f"Error ID: {row[1]}")
            print(f"Error: {row[2]}")
            print(f"Source: {row[3]}")
            print("-" * 40)
    else:
        print("✅ No recent replication errors found")
    print()
    
    # 8. Check if initial snapshot was created
    print("8. SNAPSHOT AGENT STATUS")
    print("-" * 30)
    snapshot_query = """
    SELECT 
        publication,
        status,
        warning,
        last_distsync
    FROM distribution.dbo.MSsnapshot_agents
    """
    result = execute_query(master_conn_str, snapshot_query, fetch_all=True)
    if result:
        for row in result:
            status_desc = {
                1: "Start",
                2: "Succeed", 
                3: "In progress",
                4: "Idle",
                5: "Retry",
                6: "Fail"
            }.get(row[1], "Unknown")
            
            print(f"Publication: {row[0]}")
            print(f"Status: {status_desc}")
            print(f"Last Sync: {row[3]}")
            print()
    else:
        print("❌ No snapshot agents found")

def fix_replication_issues(config):
    """Attempt to fix common replication issues."""
    master_config = config['master_database']
    master_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={master_config['host']},{master_config['port']};UID={master_config['username']};PWD={master_config['password']};TrustServerCertificate=yes"
    master_conn_str_db = f"{master_conn_str};DATABASE={master_config['database']}"
    
    print("\n" + "=" * 60)
    print("ATTEMPTING TO FIX REPLICATION ISSUES")
    print("=" * 60)
    
    # 1. Start the snapshot agent to create initial snapshot
    print("1. Starting Snapshot Agent...")
    snapshot_sql = """
    EXEC distribution.dbo.sp_MSforce_snapshot_cleanup @publication = N'AskdTransactionalPublication'
    EXEC sp_startpublication_snapshot @publication = N'AskdTransactionalPublication'
    """
    
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(master_conn_str_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("EXEC sp_startpublication_snapshot @publication = N'AskdTransactionalPublication'")
        print("✅ Snapshot agent started successfully")
    except pyodbc.Error as ex:
        print(f"❌ Failed to start snapshot agent: {ex}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    # 2. Wait for snapshot to complete
    print("\n2. Waiting for snapshot to complete (this may take a few minutes)...")
    time.sleep(30)  # Wait 30 seconds initially
    
    # Check snapshot status
    for i in range(10):  # Check for up to 5 minutes
        snapshot_check = """
        SELECT 
            status,
            warning
        FROM distribution.dbo.MSsnapshot_agents
        WHERE publication = 'AskdTransactionalPublication'
        """
        result = execute_query(master_conn_str, snapshot_check)
        if result and result[0] == 2:  # Status 2 = Succeed
            print("✅ Snapshot completed successfully")
            break
        elif result and result[0] == 6:  # Status 6 = Fail
            print("❌ Snapshot failed")
            break
        else:
            print(f"   Snapshot in progress... (check {i+1}/10)")
            time.sleep(30)
    
    # 3. Start distribution agents
    print("\n3. Starting Distribution Agents...")
    dist_agent_sql = """
    EXEC sp_start_job @job_name = N'REPL-LogReader'
    """
    try:
        conn = pyodbc.connect(master_conn_str)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("EXEC msdb.dbo.sp_start_job @job_name = (SELECT TOP 1 name FROM msdb.dbo.sysjobs WHERE name LIKE '%LogReader%')")
        print("✅ Log Reader Agent started")
    except pyodbc.Error as ex:
        print(f"⚠️ Could not start Log Reader Agent: {ex}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        config = load_config()
        diagnose_replication(config)
        
        # Ask user if they want to attempt fixes
        fix_input = input("\nWould you like to attempt to fix the identified issues? (y/n): ")
        if fix_input.lower() == 'y':
            fix_replication_issues(config)
            
            # Wait a bit and then re-run verification
            print("\nWaiting 30 seconds before re-verification...")
            time.sleep(30)
            
            print("\nRe-running verification...")
            import subprocess
            subprocess.run(["python", "verify_replication.py"])
        
    except FileNotFoundError:
        print("Error: replication_config_enhanced.json not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


