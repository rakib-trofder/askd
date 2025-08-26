#!/usr/bin/env python3
"""
Prerequisites Validation Script for Raw Replication Manager

This script validates all prerequisites before running the replication manager:
- SQL Server instances accessibility
- Database existence and structure
- Required permissions
- SQL Server Agent status
- Network connectivity
"""

import os
import sys
import json
import pyodbc
from datetime import datetime

class PrerequisitesValidator:
    def __init__(self, config_file='replication_config_enhanced.json'):
        self.config_file = config_file
        self.config = self.load_config()
        self.validation_results = {}
        
    def load_config(self):
        """Load configuration file"""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ Configuration file not found: {self.config_file}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in configuration: {e}")
            sys.exit(1)
            
    def get_connection_string(self, db_config, database=None):
        """Generate connection string"""
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['host']},{db_config['port']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']};"
            f"TrustServerCertificate=yes;"
        )
        if database:
            conn_str += f"DATABASE={database};"
        return conn_str
        
    def test_connection(self, db_config, database=None):
        """Test database connection"""
        try:
            conn_str = self.get_connection_string(db_config, database)
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@SERVERNAME, @@VERSION")
                result = cursor.fetchone()
                return True, result[0], result[1]
        except Exception as e:
            return False, None, str(e)
            
    def check_database_exists(self, db_config, database_name):
        """Check if database exists"""
        try:
            conn_str = self.get_connection_string(db_config)
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sys.databases WHERE name = ?", database_name)
                return cursor.fetchone() is not None
        except Exception:
            return False
            
    def check_table_exists(self, db_config, database_name, schema, table):
        """Check if table exists"""
        try:
            conn_str = self.get_connection_string(db_config, database_name)
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """, schema, table)
                return cursor.fetchone()[0] > 0
        except Exception:
            return False
            
    def check_primary_key(self, db_config, database_name, schema, table):
        """Check if table has primary key"""
        try:
            conn_str = self.get_connection_string(db_config, database_name)
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT k.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                      ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                    WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
                      AND k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ?
                """, schema, table)
                return len(cursor.fetchall()) > 0
        except Exception:
            return False
            
    def check_sql_agent_status(self, db_config):
        """Check if SQL Server Agent is running"""
        try:
            conn_str = self.get_connection_string(db_config)
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                # Try to check agent status
                cursor.execute("""
                    SELECT dss.[status], dss.[status_desc]
                    FROM sys.dm_server_services dss
                    WHERE dss.[servicename] LIKE N'SQL Server Agent%'
                """)
                result = cursor.fetchone()
                if result:
                    return result[0] == 4, result[1]  # 4 = Running
                else:
                    return False, "SQL Server Agent service not found"
        except Exception as e:
            return False, str(e)
            
    def check_permissions(self, db_config, database_name):
        """Check user permissions"""
        try:
            conn_str = self.get_connection_string(db_config, database_name)
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                
                # Check if user is sysadmin or db_owner using simplified query
                cursor.execute("""
                    SELECT 
                        IS_SRVROLEMEMBER('sysadmin') as is_sysadmin,
                        IS_MEMBER('db_owner') as is_db_owner,
                        SUSER_NAME() as current_user
                """)
                result = cursor.fetchone()
                
                if not result:
                    return False, "Unknown", "Unable to check permissions"
                
                has_permissions = result[0] == 1 or result[1] == 1
                current_user = result[2] if result[2] else db_config.get('username', 'Unknown')
                
                if result[0] == 1:
                    role = "sysadmin"
                elif result[1] == 1:
                    role = "db_owner"
                else:
                    role = "insufficient"
                    
                return has_permissions, current_user, role
                
        except Exception as e:
            # If the main query fails, try a simpler approach
            try:
                conn_str = self.get_connection_string(db_config, 'master')
                with pyodbc.connect(conn_str, timeout=10) as conn:
                    cursor = conn.cursor()
                    
                    # Check only sysadmin role on master database
                    cursor.execute("SELECT IS_SRVROLEMEMBER('sysadmin'), SUSER_NAME()")
                    result = cursor.fetchone()
                    
                    if result:
                        is_sysadmin = result[0] == 1
                        current_user = result[1] if result[1] else db_config.get('username', 'Unknown')
                        
                        role = "sysadmin" if is_sysadmin else "insufficient"
                        return is_sysadmin, current_user, role
                    else:
                        return False, db_config.get('username', 'Unknown'), "Unable to check permissions"
                        
            except Exception as e2:
                # Final fallback - assume user from config
                username = db_config.get('username', 'Unknown')
                error_msg = str(e2) if len(str(e2)) < 100 else str(e2)[:100] + "..."
                return False, username, f"Permission check failed: {error_msg}"
            
    def validate_master_database(self):
        """Validate master database prerequisites"""
        print("\n🔍 Validating Master Database Prerequisites...")
        
        master = self.config['master_database']
        
        # Test connection
        success, server_name, version = self.test_connection(master)
        if not success:
            print(f"❌ Cannot connect to master database: {version}")
            print("   👉 Check connection details in replication_config_enhanced.json")
            print("   👉 Verify network connectivity and firewall settings")
            return False
            
        print(f"✅ Master connection successful: {server_name}")
        print(f"   Version: {version[:80]}...")
        
        # Check permissions first (using master database)
        has_perms, username, role = self.check_permissions(master, 'master')
        if not has_perms:
            print(f"❌ Insufficient permissions for user '{username}': {role}")
            print("   👉 User needs 'sysadmin' server role or 'db_owner' database role")
            print("   👉 Grant permissions with: EXEC sp_addsrvrolemember 'username', 'sysadmin'")
            return False
            
        print(f"✅ User '{username}' has sufficient permissions: {role}")
        
        # Check if database exists
        db_exists = self.check_database_exists(master, master['database'])
        if not db_exists:
            print(f"❌ Database '{master['database']}' does not exist")
            print(f"   👉 Run setup_master_prerequisites.sql first!")
            print(f"   👉 Or create database manually: CREATE DATABASE [{master['database']}]")
            return False
            
        print(f"✅ Database '{master['database']}' exists")
        
        # Check SQL Server Agent
        agent_running, agent_status = self.check_sql_agent_status(master)
        if not agent_running:
            print(f"⚠️  SQL Server Agent not running: {agent_status}")
            print("   👉 Please start SQL Server Agent service")
            print("   👉 Use SQL Server Configuration Manager or run: NET START SQLSERVERAGENT")
        else:
            print(f"✅ SQL Server Agent is running: {agent_status}")
            
        # Check required tables
        schemas = self.config['schemas_to_replicate']
        missing_tables = []
        
        for schema in schemas:
            for table_config in schema['tables']:
                table_name = table_config['table_name']
                
                if not self.check_table_exists(master, master['database'], schema['schema_name'], table_name):
                    missing_tables.append(f"{schema['schema_name']}.{table_name}")
                    continue
                    
                # Check primary key
                if not self.check_primary_key(master, master['database'], schema['schema_name'], table_name):
                    print(f"❌ Table {schema['schema_name']}.{table_name} missing primary key")
                    print("   👉 Transactional replication requires primary keys on all tables")
                    return False
                    
                print(f"✅ Table {schema['schema_name']}.{table_name} exists with primary key")
                
        if missing_tables:
            print(f"❌ Missing tables: {', '.join(missing_tables)}")
            print("   👉 Run setup_master_prerequisites.sql to create tables")
            print("   👉 Or create tables manually based on your schema requirements")
            return False
            
        return True
        
    def validate_replica_databases(self):
        """Validate replica database prerequisites"""
        print("\n🔍 Validating Replica Database Prerequisites...")
        
        replicas = self.config['replica_databases']
        all_valid = True
        
        for replica in replicas:
            print(f"\n📍 Checking replica: {replica['name']}")
            
            # Test connection
            success, server_name, version = self.test_connection(replica)
            if not success:
                print(f"❌ Cannot connect to replica {replica['name']}: {version}")
                print("   👉 Check connection details in replication_config_enhanced.json")
                print("   👉 Verify network connectivity and firewall settings")
                all_valid = False
                continue
                
            print(f"✅ Replica connection successful: {server_name}")
            
            # Check permissions (using master database for server-level permissions)
            has_perms, username, role = self.check_permissions(replica, 'master')
            if not has_perms:
                print(f"❌ Insufficient permissions for user '{username}': {role}")
                print("   👉 User needs 'sysadmin' server role for replication setup")
                print("   👉 Grant permissions with: EXEC sp_addsrvrolemember 'username', 'sysadmin'")
                all_valid = False
                continue
                
            print(f"✅ User '{username}' has sufficient permissions: {role}")
            
            # Check SQL Server Agent
            agent_running, agent_status = self.check_sql_agent_status(replica)
            if not agent_running:
                print(f"⚠️  SQL Server Agent not running: {agent_status}")
                print("   👉 Please start SQL Server Agent service")
                print("   👉 Use SQL Server Configuration Manager or run: NET START SQLSERVERAGENT")
            else:
                print(f"✅ SQL Server Agent is running: {agent_status}")
                
            # Note: Replica databases will be created automatically by the script
            print(f"   📝 Note: Replica database will be created automatically")
            
        return all_valid
        
    def validate_network_connectivity(self):
        """Validate network connectivity between master and replicas"""
        print("\n🔍 Validating Network Connectivity...")
        
        master = self.config['master_database']
        replicas = self.config['replica_databases']
        
        # Test master to replica connectivity
        for replica in replicas:
            try:
                # This is a simplified connectivity test
                # In production, you might want to test actual network ports
                master_conn = self.test_connection(master)[0]
                replica_conn = self.test_connection(replica)[0]
                
                if master_conn and replica_conn:
                    print(f"✅ Network connectivity: Master ↔ {replica['name']}")
                else:
                    print(f"❌ Network connectivity issue: Master ↔ {replica['name']}")
                    return False
                    
            except Exception as e:
                print(f"❌ Network test failed for {replica['name']}: {e}")
                return False
                
        return True
        
    def validate_configuration(self):
        """Validate configuration file"""
        print("\n🔍 Validating Configuration...")
        
        required_sections = ['replication', 'master_database', 'replica_databases', 'schemas_to_replicate']
        
        for section in required_sections:
            if section not in self.config:
                print(f"❌ Missing configuration section: {section}")
                return False
            print(f"✅ Configuration section '{section}' found")
            
        # Validate replication settings
        replication = self.config['replication']
        if not replication.get('enabled', True):
            print("⚠️  Replication is disabled in configuration")
            
        sync_interval = replication.get('sync_interval_seconds', 15)
        if sync_interval < 5:
            print(f"⚠️  Very short sync interval: {sync_interval}s (may cause performance issues)")
        elif sync_interval > 300:
            print(f"⚠️  Long sync interval: {sync_interval}s (may cause data delays)")
        else:
            print(f"✅ Sync interval: {sync_interval}s")
            
        return True
        
    def run_full_validation(self):
        """Run complete prerequisites validation"""
        print("🚀 Prerequisites Validation for Raw Replication Manager")
        print("=" * 60)
        print(f"Started at: {datetime.now()}")
        
        validations = [
            ("Configuration", self.validate_configuration),
            ("Master Database", self.validate_master_database),
            ("Replica Databases", self.validate_replica_databases),
            ("Network Connectivity", self.validate_network_connectivity),
        ]
        
        all_passed = True
        
        for name, validation_func in validations:
            try:
                result = validation_func()
                self.validation_results[name] = result
                if not result:
                    all_passed = False
            except Exception as e:
                print(f"❌ {name} validation failed with error: {e}")
                self.validation_results[name] = False
                all_passed = False
                
        # Summary
        print("\n" + "=" * 60)
        print("📊 VALIDATION SUMMARY")
        print("=" * 60)
        
        for name, result in self.validation_results.items():
            status = "PASS" if result else "FAIL"
            icon = "✅" if result else "❌"
            print(f"{icon} {name:<20} {status}")
            
        if all_passed:
            print("\n🎉 ALL PREREQUISITES VALIDATED!")
            print("✅ Ready to run: python raw_replication_manager.py")
            return True
        else:
            print("\n🚨 PREREQUISITES VALIDATION FAILED!")
            print("❌ Please fix the issues above before proceeding.")
            print("\n📋 Common fixes:")
            print("   • Run setup_master_prerequisites.sql on master database")
            print("   • Start SQL Server Agent service on all instances")
            print("   • Verify network connectivity and firewall settings")
            print("   • Check user permissions (sysadmin or db_owner required)")
            return False

def main():
    """Main validation execution"""
    print("Prerequisites Validation for Raw Replication Manager")
    
    # Check if config file exists
    if not os.path.exists('replication_config_enhanced.json'):
        print("❌ Configuration file 'replication_config_enhanced.json' not found")
        return 1
        
    try:
        validator = PrerequisitesValidator()
        success = validator.run_full_validation()
        return 0 if success else 1
        
    except Exception as e:
        print(f"❌ Validation failed with error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())