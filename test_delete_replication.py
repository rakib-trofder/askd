#!/usr/bin/env python3
"""
Test Script for DELETE Replication

This script demonstrates how the enhanced replication manager handles DELETE operations.
"""

import pyodbc
import time

def test_delete_replication():
    """Test DELETE operations replication"""
    
    # Connection configurations
    master_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=202.4.127.187,3433;"
        "DATABASE=askd;"
        "UID=sa;"
        "PWD=YourStrongPassword!123;"
        "TrustServerCertificate=yes;"
    )
    
    replica_conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=202.4.127.187,3434;"
        "DATABASE=askd;"
        "UID=sa;"
        "PWD=YourStrongPassword!123;"
        "TrustServerCertificate=yes;"
    )
    
    try:
        print("=== Testing DELETE Replication ===")
        
        # Step 1: Check initial counts
        print("\n1. Checking initial record counts...")
        
        with pyodbc.connect(master_conn_str) as master_conn:
            cursor = master_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.Employees")
            master_count_before = cursor.fetchone()[0]
            print(f"Master Employees count: {master_count_before}")
        
        with pyodbc.connect(replica_conn_str) as replica_conn:
            cursor = replica_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.Employees")
            replica_count_before = cursor.fetchone()[0]
            print(f"Replica Employees count: {replica_count_before}")
        
        # Step 2: Delete records from master
        print("\n2. Deleting 5 records from master database...")
        
        with pyodbc.connect(master_conn_str) as master_conn:
            cursor = master_conn.cursor()
            # Delete the last 5 employees
            cursor.execute("""
                DELETE TOP(5) FROM dbo.Employees 
                WHERE EmployeeID IN (
                    SELECT TOP(5) EmployeeID FROM dbo.Employees 
                    ORDER BY EmployeeID DESC
                )
            """)
            deleted_count = cursor.rowcount
            master_conn.commit()
            print(f"Deleted {deleted_count} records from master")
        
        # Step 3: Check master count after deletion
        with pyodbc.connect(master_conn_str) as master_conn:
            cursor = master_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.Employees")
            master_count_after = cursor.fetchone()[0]
            print(f"Master Employees count after deletion: {master_count_after}")
        
        # Step 4: Wait for replication
        print("\n3. Waiting for replication to sync (60 seconds)...")
        print("   Make sure the enhanced replication manager is running!")
        for i in range(60, 0, -10):
            print(f"   Waiting {i} seconds...")
            time.sleep(10)
        
        # Step 5: Check replica count after replication
        print("\n4. Checking replica count after replication...")
        
        with pyodbc.connect(replica_conn_str) as replica_conn:
            cursor = replica_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.Employees")
            replica_count_after = cursor.fetchone()[0]
            print(f"Replica Employees count after replication: {replica_count_after}")
        
        # Step 6: Verify DELETE replication
        print("\n5. Verification Results:")
        print(f"   Master count before: {master_count_before}")
        print(f"   Master count after:  {master_count_after}")
        print(f"   Replica count before: {replica_count_before}")
        print(f"   Replica count after:  {replica_count_after}")
        
        if master_count_after == replica_count_after:
            print("   ✅ DELETE replication SUCCESSFUL! Counts match.")
        else:
            print("   ❌ DELETE replication FAILED! Counts don't match.")
            print(f"   Expected replica count: {master_count_after}")
            print(f"   Actual replica count: {replica_count_after}")
        
        # Step 7: Show detailed comparison
        print("\n6. Detailed record comparison:")
        
        with pyodbc.connect(master_conn_str) as master_conn:
            cursor = master_conn.cursor()
            cursor.execute("SELECT EmployeeID, FirstName, LastName FROM dbo.Employees ORDER BY EmployeeID")
            master_records = cursor.fetchall()
        
        with pyodbc.connect(replica_conn_str) as replica_conn:
            cursor = replica_conn.cursor()
            cursor.execute("SELECT EmployeeID, FirstName, LastName FROM dbo.Employees ORDER BY EmployeeID")
            replica_records = cursor.fetchall()
        
        master_ids = {r[0] for r in master_records}
        replica_ids = {r[0] for r in replica_records}
        
        missing_in_replica = master_ids - replica_ids
        extra_in_replica = replica_ids - master_ids
        
        if missing_in_replica:
            print(f"   Records in master but missing in replica: {missing_in_replica}")
        if extra_in_replica:
            print(f"   Records in replica but not in master: {extra_in_replica}")
        if not missing_in_replica and not extra_in_replica:
            print("   ✅ All records match perfectly!")
        
        return master_count_after == replica_count_after
        
    except Exception as e:
        print(f"Error during test: {e}")
        return False

def main():
    """Main test function"""
    print("DELETE Replication Test")
    print("=" * 50)
    print("This test will:")
    print("1. Check current record counts")
    print("2. Delete 5 records from master")
    print("3. Wait for replication")
    print("4. Verify the deletes were replicated")
    print()
    
    response = input("Do you want to proceed? (y/N): ")
    if response.lower() != 'y':
        print("Test cancelled.")
        return
    
    success = test_delete_replication()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 DELETE REPLICATION TEST PASSED!")
    else:
        print("❌ DELETE REPLICATION TEST FAILED!")
    print("=" * 50)

if __name__ == "__main__":
    main()