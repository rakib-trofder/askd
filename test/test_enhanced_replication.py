#!/usr/bin/env python3
"""
Enhanced Replication Test Runner

This script tests the enhanced SQL Server replication manager with your specific configuration.
"""

import os
import sys
import subprocess

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import pyodbc
        import pandas as pd
        print("✓ Required Python packages are installed")
        return True
    except ImportError as e:
        print(f"✗ Missing required package: {e}")
        print("Please install dependencies with: pip install -r requirements.txt")
        return False

def check_sql_server_drivers():
    """Check if SQL Server ODBC drivers are available"""
    try:
        import pyodbc
        drivers = [driver for driver in pyodbc.drivers() if 'SQL Server' in driver]
        if drivers:
            print(f"✓ SQL Server ODBC drivers found: {drivers}")
            return True
        else:
            print("✗ No SQL Server ODBC drivers found")
            print("Please install Microsoft ODBC Driver for SQL Server")
            return False
    except Exception as e:
        print(f"✗ Error checking ODBC drivers: {e}")
        return False

def test_sql_connections():
    """Test connections to SQL Server instances"""
    print("Testing SQL Server connections...")
    
    # Test configurations from your provided setup
    test_configs = [
        {"host": "202.4.127.187", "port": 3433, "name": "Master", "database": "askd"},
        {"host": "202.4.127.187", "port": 3434, "name": "Alpha Replica", "database": "askd"},
        {"host": "202.4.127.187", "port": 3435, "name": "Beta Replica", "database": "askd"}
    ]
    
    import pyodbc
    
    for config in test_configs:
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={config['host']},{config['port']};"
                f"DATABASE=master;"  # Connect to master first to test connection
                f"UID=sa;"
                f"PWD=YourStrongPassword!123;"
                f"TrustServerCertificate=yes;"
            )
            
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@SERVERNAME, @@VERSION")
                result = cursor.fetchone()
                print(f"✓ {config['name']} connection successful")
                
                # Check if target database exists
                cursor.execute("SELECT name FROM sys.databases WHERE name = ?", config['database'])
                db_exists = cursor.fetchone()
                if db_exists:
                    print(f"  └─ Database '{config['database']}' exists")
                else:
                    print(f"  └─ Database '{config['database']}' not found (will be created automatically)")
                
        except Exception as e:
            print(f"✗ {config['name']} connection failed: {e}")
            return False
    
    return True

def run_master_setup():
    """Offer to run master database setup"""
    print("\n" + "=" * 60)
    print("Master Database Setup")
    print("=" * 60)
    
    response = input("Do you want to run the master database setup script? (y/N): ")
    if response.lower() == 'y':
        try:
            import pyodbc
            
            # Read the setup script
            setup_script_path = 'setup_master_database.sql'
            if not os.path.exists(setup_script_path):
                print(f"Setup script not found: {setup_script_path}")
                return False
            
            with open(setup_script_path, 'r') as f:
                setup_sql = f.read()
            
            # Connect to master database server
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=202.4.127.187,3433;"
                "DATABASE=master;"
                "UID=sa;"
                "PWD=YourStrongPassword!123;"
                "TrustServerCertificate=yes;"
            )
            
            print("Executing master database setup...")
            with pyodbc.connect(conn_str, timeout=30) as conn:
                # Split script by GO statements
                statements = setup_sql.split('GO')
                cursor = conn.cursor()
                
                for statement in statements:
                    statement = statement.strip()
                    if statement:
                        try:
                            cursor.execute(statement)
                            cursor.commit()
                        except Exception as e:
                            print(f"Warning: {e}")
                            
            print("✓ Master database setup completed")
            
            # Ask about dummy data
            response = input("Do you want to insert dummy data? (y/N): ")
            if response.lower() == 'y':
                dummy_script_path = 'insert_dummy_data.sql'
                if os.path.exists(dummy_script_path):
                    with open(dummy_script_path, 'r') as f:
                        dummy_sql = f.read()
                    
                    # Connect to askd database
                    conn_str = conn_str.replace("DATABASE=master;", "DATABASE=askd;")
                    
                    print("Inserting dummy data...")
                    with pyodbc.connect(conn_str, timeout=30) as conn:
                        statements = dummy_sql.split('GO')
                        cursor = conn.cursor()
                        
                        for statement in statements:
                            statement = statement.strip()
                            if statement:
                                try:
                                    cursor.execute(statement)
                                    cursor.commit()
                                except Exception as e:
                                    print(f"Warning: {e}")
                                    
                    print("✓ Dummy data inserted successfully")
                else:
                    print(f"Dummy data script not found: {dummy_script_path}")
                    
            return True
            
        except Exception as e:
            print(f"Error running master setup: {e}")
            return False
    
    return True

def main():
    """Main test runner"""
    print("Enhanced SQL Server Replication Manager - Test Runner")
    print("=" * 60)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check ODBC drivers
    if not check_sql_server_drivers():
        sys.exit(1)
    
    # Test SQL connections
    if not test_sql_connections():
        print("\n⚠️  SQL Server connection tests failed.")
        print("Please ensure the SQL Server instances are running and accessible.")
        response = input("Do you want to continue anyway? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    # Offer master database setup
    if not run_master_setup():
        print("Master database setup failed.")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("Starting Enhanced Replication Manager...")
    print("Configuration: replication_config_enhanced.json")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    
    # Set environment variable for enhanced config
    os.environ['CONFIG_FILE'] = 'replication_config_enhanced.json'
    
    # Import and run the enhanced replication manager
    try:
        from enhanced_replication_manager import EnhancedSQLServerReplicationManager
        
        config_file = 'replication_config_enhanced.json'
        if not os.path.exists(config_file):
            print(f"Configuration file not found: {config_file}")
            sys.exit(1)
            
        manager = EnhancedSQLServerReplicationManager(config_file)
        manager.start()
        
    except KeyboardInterrupt:
        print("\nStopping enhanced replication manager...")
    except Exception as e:
        print(f"\nError running enhanced replication manager: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()