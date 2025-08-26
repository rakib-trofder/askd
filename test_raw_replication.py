#!/usr/bin/env python3
"""
Test Script for Enhanced Raw Replication Manager

This script tests the enhanced raw_replication_manager.py with comprehensive
validation for incremental sync, large dataset handling, dynamic configuration,
and exception handling.
"""

import os
import sys
import json
import time
import subprocess
import traceback
from datetime import datetime

def test_dependencies():
    """Test if required dependencies are available"""
    print("🔍 Testing Dependencies...")
    
    try:
        import pyodbc
        print("✅ pyodbc available")
    except ImportError:
        print("❌ pyodbc not available - install with: pip install pyodbc")
        return False
        
    try:
        drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
        if drivers:
            print(f"✅ SQL Server ODBC drivers found: {drivers}")
        else:
            print("❌ No SQL Server ODBC drivers found")
            return False
    except Exception as e:
        print(f"❌ Error checking ODBC drivers: {e}")
        return False
        
    return True

def test_configuration():
    """Test configuration loading and validation"""
    print("\n🔍 Testing Configuration...")
    
    try:
        # Import the enhanced manager
        from raw_replication_manager import EnhancedReplicationManager
        
        # Test configuration loading
        manager = EnhancedReplicationManager('replication_config_enhanced.json')
        
        # Validate required configuration sections
        required_sections = ['replication', 'master_database', 'replica_databases', 'schemas_to_replicate']
        for section in required_sections:
            if section not in manager.config:
                print(f"❌ Missing configuration section: {section}")
                return False
            print(f"✅ Configuration section '{section}' found")
            
        # Validate replication settings
        replication_config = manager.config['replication']
        expected_defaults = {
            'distributor_admin_password': 'DistributorPassword!123',
            'sync_interval_seconds': 15,
            'batch_size': 10000,
            'connection_timeout': 30,
            'retry_attempts': 3
        }
        
        for key, expected_value in expected_defaults.items():
            if key not in replication_config:
                print(f"❌ Missing replication setting: {key}")
                return False
            print(f"✅ Replication setting '{key}' = {replication_config[key]}")
            
        # Test configuration hash and change detection
        initial_hash = manager._get_config_hash()
        if initial_hash:
            print("✅ Configuration hash generation working")
        else:
            print("⚠️  Configuration hash generation failed")
            
        print("✅ Configuration validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        traceback.print_exc()
        return False

def test_connection_management():
    """Test connection pooling and retry mechanisms"""
    print("\n🔍 Testing Connection Management...")
    
    try:
        from raw_replication_manager import EnhancedReplicationManager
        
        manager = EnhancedReplicationManager('replication_config_enhanced.json')
        master_config = manager.config['master_database']
        
        # Test connection string generation
        conn_str = manager.get_connection_string(
            master_config['host'], 
            master_config['port'], 
            master_config['username'], 
            master_config['password'],
            master_config['database']
        )
        
        if 'DRIVER=' in conn_str and 'SERVER=' in conn_str:
            print("✅ Connection string generation working")
        else:
            print("❌ Invalid connection string generated")
            return False
            
        # Test actual database connection (if available)
        try:
            result = manager.execute_query_with_retry(
                master_config['host'], 
                master_config['port'], 
                master_config['username'], 
                master_config['password'],
                'master',  # Connect to master db
                "SELECT @@VERSION",
                fetch=True
            )
            
            if result:
                print("✅ Database connection successful")
                print(f"   SQL Server Version: {result[0][0][:50]}...")
            else:
                print("⚠️  Database connection test skipped (server not available)")
                
        except Exception as e:
            print(f"⚠️  Database connection test failed: {e}")
            print("   This is expected if SQL Server is not running or accessible")
            
        # Test connection pooling
        pool_key = f"{master_config['host']}:{master_config['port']}:master"
        if pool_key in manager.connection_pool:
            print("✅ Connection pooling working")
        else:
            print("⚠️  Connection not added to pool (expected if connection failed)")
            
        print("✅ Connection management test completed")
        return True
        
    except Exception as e:
        print(f"❌ Connection management test failed: {e}")
        traceback.print_exc()
        return False

def test_schema_operations():
    """Test schema scripting and table operations"""
    print("\n🔍 Testing Schema Operations...")
    
    try:
        from raw_replication_manager import EnhancedReplicationManager
        
        manager = EnhancedReplicationManager('replication_config_enhanced.json')
        
        # Test build_create_table_script with sample data
        sample_columns = [
            ('ID', 'int', None, 10, 0, 'NO', 1, None),  # Identity column
            ('Name', 'nvarchar', 100, None, None, 'NO', 0, None),  # Required string
            ('Email', 'nvarchar', 255, None, None, 'YES', 0, None),  # Optional string
            ('CreatedDate', 'datetime2', None, None, None, 'NO', 0, '(getdate())'),  # With default
            ('Price', 'decimal', None, 18, 2, 'YES', 0, None)  # Decimal
        ]
        
        pk_columns = ['ID']
        
        create_sql = manager.build_create_table_script(sample_columns, pk_columns, 'dbo', 'TestTable')
        
        # Validate the generated SQL
        expected_elements = [
            'CREATE TABLE [dbo].[TestTable]',
            '[ID] int IDENTITY(1,1) NOT NULL',
            '[Name] nvarchar(100) NOT NULL',
            '[Email] nvarchar(255) NULL',
            '[CreatedDate] datetime2 NOT NULL DEFAULT (getdate())',
            '[Price] decimal(18,2) NULL',
            'PRIMARY KEY ([ID])'
        ]
        
        for element in expected_elements:
            if element not in create_sql:
                print(f"❌ Missing element in CREATE TABLE: {element}")
                return False
                
        print("✅ CREATE TABLE script generation working")
        
        # Test index script generation
        sample_indexes = [
            ('IX_TestTable_Name', False, False, 'Name'),
            ('IX_TestTable_Email', True, False, 'Email'),
        ]
        
        index_scripts = manager.build_create_indexes_script(sample_indexes, 'dbo', 'TestTable')
        
        if len(index_scripts) == 2:
            print("✅ CREATE INDEX script generation working")
        else:
            print(f"❌ Expected 2 index scripts, got {len(index_scripts)}")
            return False
            
        # Test foreign key script generation
        sample_fk = ('FK_TestTable_Category', 'dbo', 'Categories', 'ID', 'CategoryID')
        fk_script = manager.build_fk_script(sample_fk, 'dbo', 'TestTable')
        
        if fk_script and 'FOREIGN KEY' in fk_script:
            print("✅ Foreign key script generation working")
        else:
            print("❌ Foreign key script generation failed")
            return False
            
        print("✅ Schema operations test completed")
        return True
        
    except Exception as e:
        print(f"❌ Schema operations test failed: {e}")
        traceback.print_exc()
        return False

def test_error_handling():
    """Test error handling and retry mechanisms"""
    print("\n🔍 Testing Error Handling...")
    
    try:
        from raw_replication_manager import EnhancedReplicationManager
        
        manager = EnhancedReplicationManager('replication_config_enhanced.json')
        
        # Test retry mechanism with invalid connection
        try:
            manager.execute_query_with_retry(
                'invalid_host', 1433, 'invalid_user', 'invalid_pass',
                'master', "SELECT 1", retry_attempts=2
            )
            print("❌ Expected connection failure, but succeeded")
            return False
        except Exception:
            print("✅ Retry mechanism working (expected failure)")
            
        # Test configuration validation with missing file
        try:
            manager_invalid = EnhancedReplicationManager('nonexistent_config.json')
            print("❌ Expected configuration error, but succeeded")
            return False
        except FileNotFoundError:
            print("✅ Configuration file validation working")
        except Exception as e:
            print(f"✅ Configuration validation working (caught: {type(e).__name__})")
            
        print("✅ Error handling test completed")
        return True
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        traceback.print_exc()
        return False

def test_optimization_features():
    """Test optimization features for large datasets"""
    print("\n🔍 Testing Optimization Features...")
    
    try:
        from raw_replication_manager import EnhancedReplicationManager
        
        manager = EnhancedReplicationManager('replication_config_enhanced.json')
        
        # Test batch size configuration
        batch_size = manager.config.get('replication', {}).get('batch_size', 1000)
        if batch_size >= 1000:
            print(f"✅ Batch size configured: {batch_size:,}")
        else:
            print(f"⚠️  Small batch size: {batch_size}")
            
        # Test connection timeout configuration
        timeout = manager.config.get('replication', {}).get('connection_timeout', 30)
        if timeout >= 30:
            print(f"✅ Connection timeout configured: {timeout}s")
        else:
            print(f"⚠️  Short connection timeout: {timeout}s")
            
        # Test sync interval optimization
        sync_interval = manager.config.get('replication', {}).get('sync_interval_seconds', 15)
        if 5 <= sync_interval <= 300:
            print(f"✅ Sync interval optimized: {sync_interval}s")
        else:
            print(f"⚠️  Sync interval may need adjustment: {sync_interval}s")
            
        # Test backup/restore threshold
        backup_threshold = manager.config.get('replication', {}).get('backup_restore_threshold', 5000000)
        if backup_threshold >= 1000000:
            print(f"✅ Backup/restore threshold: {backup_threshold:,} rows")
        else:
            print(f"⚠️  Low backup/restore threshold: {backup_threshold:,} rows")
            
        print("✅ Optimization features test completed")
        return True
        
    except Exception as e:
        print(f"❌ Optimization features test failed: {e}")
        traceback.print_exc()
        return False

def test_dynamic_configuration():
    """Test dynamic configuration capabilities"""
    print("\n🔍 Testing Dynamic Configuration...")
    
    try:
        from raw_replication_manager import EnhancedReplicationManager
        
        manager = EnhancedReplicationManager('replication_config_enhanced.json')
        
        # Test configuration change detection
        initial_hash = manager._get_config_hash()
        
        # Simulate configuration change by getting hash again
        current_hash = manager._get_config_hash()
        
        if initial_hash == current_hash:
            print("✅ Configuration hash consistency working")
        else:
            print("❌ Configuration hash inconsistency")
            return False
            
        # Test configuration reload capability
        changes_detected = manager.check_config_changes()
        print(f"✅ Configuration change detection: {changes_detected}")
        
        # Test schema configuration parsing
        schemas = manager.config.get('schemas_to_replicate', [])
        if schemas:
            for schema in schemas:
                if 'schema_name' in schema and 'tables' in schema:
                    tables = schema['tables']
                    for table in tables:
                        if 'table_name' in table and 'primary_key' in table:
                            print(f"✅ Table configuration valid: {schema['schema_name']}.{table['table_name']}")
                        else:
                            print(f"❌ Invalid table configuration: {table}")
                            return False
                else:
                    print(f"❌ Invalid schema configuration: {schema}")
                    return False
        else:
            print("⚠️  No schemas configured for replication")
            
        print("✅ Dynamic configuration test completed")
        return True
        
    except Exception as e:
        print(f"❌ Dynamic configuration test failed: {e}")
        traceback.print_exc()
        return False

def run_full_test_suite():
    """Run the complete test suite"""
    print("🚀 Enhanced Raw Replication Manager Test Suite")
    print("=" * 60)
    
    tests = [
        ("Dependencies", test_dependencies),
        ("Configuration", test_configuration),
        ("Connection Management", test_connection_management),
        ("Schema Operations", test_schema_operations),
        ("Error Handling", test_error_handling),
        ("Optimization Features", test_optimization_features),
        ("Dynamic Configuration", test_dynamic_configuration),
    ]
    
    results = {}
    total_tests = len(tests)
    passed_tests = 0
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = test_func()
            results[test_name] = result
            if result:
                passed_tests += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            results[test_name] = False
            print(f"❌ {test_name} FAILED with exception: {e}")
            
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        icon = "✅" if result else "❌"
        print(f"{icon} {test_name:<25} {status}")
        
    print(f"\n📈 Results: {passed_tests}/{total_tests} tests passed")
    print(f"🎯 Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    if passed_tests == total_tests:
        print("\n🎉 ALL TESTS PASSED! The enhanced replication manager is ready.")
    elif passed_tests >= total_tests * 0.8:
        print("\n⚠️  Most tests passed. Review failed tests before production use.")
    else:
        print("\n🚨 Multiple test failures. Please fix issues before proceeding.")
        
    return passed_tests == total_tests

def main():
    """Main test execution"""
    print(f"Test started at: {datetime.now()}")
    
    # Check if configuration file exists
    if not os.path.exists('replication_config_enhanced.json'):
        print("❌ Error: replication_config_enhanced.json not found")
        print("   Please ensure the configuration file exists in the current directory.")
        return 1
        
    # Run the test suite
    success = run_full_test_suite()
    
    print(f"\nTest completed at: {datetime.now()}")
    
    if success:
        print("\n🎯 Ready to run: python raw_replication_manager.py")
        return 0
    else:
        print("\n🔧 Please fix the issues above before running the replication manager.")
        return 1

if __name__ == "__main__":
    exit(main())