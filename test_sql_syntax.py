#!/usr/bin/env python3
"""
Quick SQL Syntax Test for Permission Checking

This script tests the SQL syntax fixes for the permission checking functions.
"""

import os
import sys
import json
import pyodbc

def test_sql_syntax():
    """Test the SQL syntax for permission checking"""
    print("🔍 Testing SQL Syntax Fix...")
    
    # Load configuration
    if not os.path.exists('replication_config_enhanced.json'):
        print("❌ Configuration file not found")
        return False
        
    with open('replication_config_enhanced.json', 'r') as f:
        config = json.load(f)
        
    master = config['master_database']
    
    # Build connection string
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={master['host']},{master['port']};"
        f"UID={master['username']};"
        f"PWD={master['password']};"
        f"TrustServerCertificate=yes;"
    )
    
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            
            print("✅ Connection successful")
            
            # Test the fixed SQL syntax
            print("🧪 Testing fixed permission query...")
            cursor.execute("""
                SELECT 
                    IS_SRVROLEMEMBER('sysadmin') as is_sysadmin,
                    IS_MEMBER('db_owner') as is_db_owner,
                    SYSTEM_USER as current_user
            """)
            result = cursor.fetchone()
            
            if result:
                print(f"✅ Permission query successful:")
                print(f"   • Is sysadmin: {result[0] == 1}")
                print(f"   • Is db_owner: {result[1] == 1}")
                print(f"   • Current user: {result[2]}")
                return True
            else:
                print("❌ No result from permission query")
                return False
                
    except Exception as e:
        print(f"❌ SQL syntax test failed: {e}")
        
        # Try alternative syntax
        try:
            print("🧪 Testing alternative permission query...")
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        IS_SRVROLEMEMBER('sysadmin') as is_sysadmin,
                        1 as is_db_owner,
                        SUSER_NAME() as current_user
                """)
                result = cursor.fetchone()
                
                if result:
                    print(f"✅ Alternative permission query successful:")
                    print(f"   • Is sysadmin: {result[0] == 1}")
                    print(f"   • Current user: {result[2]}")
                    return True
                else:
                    print("❌ Alternative query also failed")
                    return False
                    
        except Exception as e2:
            print(f"❌ Alternative syntax also failed: {e2}")
            return False

def main():
    """Main test execution"""
    print("🚀 SQL Syntax Fix Validation")
    print("=" * 40)
    
    success = test_sql_syntax()
    
    if success:
        print("\n✅ SQL syntax fix validated successfully!")
        print("👉 You can now run: python validate_prerequisites.py")
    else:
        print("\n❌ SQL syntax validation failed")
        print("👉 Please check your SQL Server version and connection details")
        
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())