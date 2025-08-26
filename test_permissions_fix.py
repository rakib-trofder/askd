#!/usr/bin/env python3
"""
Test Script for Permission Check SQL Syntax Fix

This script tests the exact SQL queries used for permission checking.
"""

import os
import sys
import json
import pyodbc

def test_permission_queries():
    """Test the SQL queries for permission checking"""
    print("🔍 Testing Permission Check SQL Syntax...")
    
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
    
    print(f"Testing connection to: {master['host']}:{master['port']} as {master['username']}")
    
    try:
        # Test 1: Main permission query
        print("\n🧪 Test 1: Main permission query...")
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    IS_SRVROLEMEMBER('sysadmin') as is_sysadmin,
                    IS_MEMBER('db_owner') as is_db_owner,
                    SUSER_NAME() as current_user
            """)
            result = cursor.fetchone()
            
            if result:
                print(f"✅ Main query successful:")
                print(f"   • Is sysadmin: {result[0] == 1}")
                print(f"   • Is db_owner: {result[1] == 1}")
                print(f"   • Current user: {result[2]}")
                
                # Determine permissions
                has_permissions = result[0] == 1 or result[1] == 1
                if result[0] == 1:
                    role = "sysadmin"
                elif result[1] == 1:
                    role = "db_owner"
                else:
                    role = "insufficient"
                    
                print(f"   • Has permissions: {has_permissions}")
                print(f"   • Role: {role}")
                
                return True
            else:
                print("❌ No result from main query")
                
    except Exception as e:
        print(f"❌ Main query failed: {e}")
        
        # Test 2: Fallback query
        try:
            print("\n🧪 Test 2: Fallback query...")
            with pyodbc.connect(conn_str, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT IS_SRVROLEMEMBER('sysadmin'), SUSER_NAME()")
                result = cursor.fetchone()
                
                if result:
                    print(f"✅ Fallback query successful:")
                    print(f"   • Is sysadmin: {result[0] == 1}")
                    print(f"   • Current user: {result[1]}")
                    
                    is_sysadmin = result[0] == 1
                    role = "sysadmin" if is_sysadmin else "insufficient"
                    
                    print(f"   • Has permissions: {is_sysadmin}")
                    print(f"   • Role: {role}")
                    
                    return True
                else:
                    print("❌ No result from fallback query")
                    
        except Exception as e2:
            print(f"❌ Fallback query also failed: {e2}")
            
            # Test 3: Simplest possible query
            try:
                print("\n🧪 Test 3: Simplest query...")
                with pyodbc.connect(conn_str, timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT IS_SRVROLEMEMBER('sysadmin')")
                    result = cursor.fetchone()
                    
                    if result:
                        print(f"✅ Simplest query successful:")
                        print(f"   • Is sysadmin: {result[0] == 1}")
                        print(f"   • Current user: {master['username']} (from config)")
                        
                        is_sysadmin = result[0] == 1
                        role = "sysadmin" if is_sysadmin else "insufficient"
                        
                        print(f"   • Has permissions: {is_sysadmin}")
                        print(f"   • Role: {role}")
                        
                        return True
                    else:
                        print("❌ No result from simplest query")
                        
            except Exception as e3:
                print(f"❌ All queries failed. Final error: {e3}")
                
    return False

def main():
    """Main test execution"""
    print("🚀 Permission Check SQL Syntax Test")
    print("=" * 50)
    
    success = test_permission_queries()
    
    if success:
        print("\n✅ Permission check SQL syntax is working!")
        print("👉 You can now run: python validate_prerequisites.py")
    else:
        print("\n❌ Permission check SQL syntax test failed")
        print("👉 Check your SQL Server version and user permissions")
        print("👉 The user might need to be granted explicit permissions")
        
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())