#!/usr/bin/env python3
"""
SQL Server Connectivity Diagnostic Script

This script helps diagnose SQL Server connectivity issues, particularly
the "Named Pipes Provider: Could not open a connection" error.
"""

import json
import socket
import pyodbc
import subprocess
import sys
import os
from datetime import datetime

class SQLConnectivityDiagnostic:
    def __init__(self, config_file='replication_config_enhanced.json'):
        self.config_file = config_file
        self.config = self.load_config()
        
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
            
    def test_network_connectivity(self, host, port):
        """Test basic network connectivity to host:port"""
        try:
            socket.setdefaulttimeout(10)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                return True, "Port is open and accessible"
            else:
                return False, f"Port is not accessible (error code: {result})"
                
        except socket.gaierror as e:
            return False, f"DNS resolution failed: {e}"
        except Exception as e:
            return False, f"Network test failed: {e}"
            
    def test_telnet_connectivity(self, host, port):
        """Test connectivity using telnet command"""
        try:
            # Try using telnet command (Windows)
            result = subprocess.run(
                ['telnet', host, str(port)], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            return True, "Telnet connection successful"
        except subprocess.TimeoutExpired:
            return False, "Telnet connection timed out"
        except FileNotFoundError:
            return False, "Telnet client not available"
        except Exception as e:
            return False, f"Telnet test failed: {e}"
            
    def test_sql_connection(self, host, port, username, password, database=None):
        """Test SQL Server connection with detailed error analysis"""
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host},{port};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=10;"
        )
        
        if database:
            conn_str += f"DATABASE={database};"
            
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@SERVERNAME, @@VERSION, GETDATE()")
                result = cursor.fetchone()
                
                return True, {
                    "server_name": result[0],
                    "version": result[1][:100] + "...",
                    "server_time": result[2],
                    "connection_string": conn_str.replace(password, "***")
                }
                
        except pyodbc.Error as e:
            error_code = e.args[0] if e.args else "Unknown"
            error_msg = e.args[1] if len(e.args) > 1 else str(e)
            
            # Analyze specific errors
            analysis = self.analyze_sql_error(error_code, error_msg)
            
            return False, {
                "error_code": error_code,
                "error_message": error_msg,
                "analysis": analysis,
                "connection_string": conn_str.replace(password, "***")
            }
            
    def analyze_sql_error(self, error_code, error_msg):
        """Analyze SQL Server error and provide troubleshooting suggestions"""
        error_msg_lower = error_msg.lower()
        
        if "named pipes provider" in error_msg_lower and "could not open a connection" in error_msg_lower:
            return {
                "issue": "SQL Server service not running or not accessible",
                "possible_causes": [
                    "SQL Server service is stopped",
                    "Incorrect server name or port number",
                    "Network connectivity issues",
                    "Firewall blocking the connection",
                    "SQL Server not configured to accept remote connections"
                ],
                "troubleshooting_steps": [
                    "Verify SQL Server service is running",
                    "Check if SQL Server is listening on the specified port",
                    "Test network connectivity with telnet",
                    "Check firewall settings",
                    "Verify SQL Server configuration for remote connections"
                ]
            }
        elif "login failed" in error_msg_lower:
            return {
                "issue": "Authentication failure",
                "possible_causes": [
                    "Incorrect username or password",
                    "User account disabled or locked",
                    "SQL Server authentication not enabled"
                ],
                "troubleshooting_steps": [
                    "Verify username and password",
                    "Check if SQL Server Mixed Mode authentication is enabled",
                    "Verify user account status"
                ]
            }
        elif "timeout" in error_msg_lower:
            return {
                "issue": "Connection timeout",
                "possible_causes": [
                    "Network latency or connectivity issues",
                    "SQL Server overloaded",
                    "Firewall causing delays"
                ],
                "troubleshooting_steps": [
                    "Increase connection timeout",
                    "Check network performance",
                    "Monitor SQL Server performance"
                ]
            }
        else:
            return {
                "issue": "Unknown SQL Server error",
                "possible_causes": ["Various SQL Server configuration issues"],
                "troubleshooting_steps": ["Check SQL Server logs for more details"]
            }
            
    def check_sql_server_service(self):
        """Check SQL Server service status using Windows commands"""
        try:
            # Check SQL Server services
            result = subprocess.run(
                ['sc', 'query', 'MSSQLSERVER'], 
                capture_output=True, 
                text=True
            )
            
            if "RUNNING" in result.stdout:
                return True, "SQL Server (MSSQLSERVER) service is running"
            elif "STOPPED" in result.stdout:
                return False, "SQL Server (MSSQLSERVER) service is stopped"
            else:
                return False, f"SQL Server service status unclear: {result.stdout}"
                
        except Exception as e:
            return False, f"Unable to check service status: {e}"
            
    def check_odbc_driver(self):
        """Check if ODBC Driver 17 for SQL Server is available"""
        try:
            drivers = pyodbc.drivers()
            sql_drivers = [d for d in drivers if 'SQL Server' in d]
            
            if any('17' in d for d in sql_drivers):
                return True, f"Available SQL Server drivers: {sql_drivers}"
            else:
                return False, f"ODBC Driver 17 not found. Available: {sql_drivers}"
                
        except Exception as e:
            return False, f"Unable to check ODBC drivers: {e}"
            
    def run_comprehensive_diagnosis(self):
        """Run complete connectivity diagnosis"""
        print("🔍 SQL Server Connectivity Diagnostic Tool")
        print("=" * 60)
        print(f"Started at: {datetime.now()}")
        print()
        
        # Check ODBC driver
        print("1. 🔧 Checking ODBC Driver...")
        driver_ok, driver_msg = self.check_odbc_driver()
        print(f"   {'✅' if driver_ok else '❌'} {driver_msg}")
        print()
        
        # Check local SQL Server service (if connecting to localhost)
        print("2. 🔧 Checking Local SQL Server Service...")
        service_ok, service_msg = self.check_sql_server_service()
        print(f"   {'✅' if service_ok else '❌'} {service_msg}")
        print()
        
        # Test master database
        print("3. 🔧 Testing Master Database Connection...")
        master = self.config['master_database']
        self.diagnose_database_connection("Master", master)
        
        # Test replica databases
        print("\n4. 🔧 Testing Replica Database Connections...")
        for i, replica in enumerate(self.config['replica_databases'], 1):
            print(f"\n   📍 Replica {i}: {replica['name']}")
            self.diagnose_database_connection(f"Replica-{i}", replica)
            
        print("\n" + "=" * 60)
        print("📋 DIAGNOSIS COMPLETE")
        print("=" * 60)
        
    def diagnose_database_connection(self, db_type, db_config):
        """Diagnose connection to a specific database"""
        host = db_config['host']
        port = db_config['port']
        username = db_config['username']
        password = db_config['password']
        database = db_config.get('database')
        
        print(f"   🎯 {db_type}: {host}:{port}")
        
        # Test network connectivity
        net_ok, net_msg = self.test_network_connectivity(host, port)
        print(f"   {'✅' if net_ok else '❌'} Network: {net_msg}")
        
        if not net_ok:
            print(f"   💡 Suggestion: Check if SQL Server is running on {host}:{port}")
            print(f"   💡 Command: telnet {host} {port}")
            return
            
        # Test SQL connection to master database first
        sql_ok, sql_result = self.test_sql_connection(host, port, username, password, 'master')
        
        if sql_ok:
            print(f"   ✅ SQL Connection: Successfully connected")
            print(f"   📊 Server: {sql_result['server_name']}")
            print(f"   📊 Version: {sql_result['version']}")
            print(f"   📊 Time: {sql_result['server_time']}")
            
            # If master connection works, test specific database
            if database and database != 'master':
                db_ok, db_result = self.test_sql_connection(host, port, username, password, database)
                if db_ok:
                    print(f"   ✅ Database '{database}': Accessible")
                else:
                    print(f"   ❌ Database '{database}': {db_result['error_message']}")
                    
        else:
            print(f"   ❌ SQL Connection: Failed")
            print(f"   📋 Error Code: {sql_result['error_code']}")
            print(f"   📋 Error: {sql_result['error_message']}")
            
            # Show detailed analysis
            analysis = sql_result['analysis']
            print(f"   🔍 Issue: {analysis['issue']}")
            print(f"   🔍 Possible Causes:")
            for cause in analysis['possible_causes']:
                print(f"      • {cause}")
            print(f"   🔍 Troubleshooting Steps:")
            for step in analysis['troubleshooting_steps']:
                print(f"      • {step}")

def main():
    """Main diagnostic execution"""
    try:
        diagnostic = SQLConnectivityDiagnostic()
        diagnostic.run_comprehensive_diagnosis()
        
        print("\n🚀 Next Steps:")
        print("1. Fix any issues identified above")
        print("2. Re-run this diagnostic to verify fixes")
        print("3. Run: python raw_replication_manager.py")
        
        return 0
        
    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())