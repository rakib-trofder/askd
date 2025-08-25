# Enhanced SQL Server Replication System

This enhanced replication system provides automatic setup and management of SQL Server master-replica replication with dynamic configuration support. The system can automatically create replica databases, schemas, and tables based on the master database structure.

## 🎯 **Key Features**

### **Auto-Setup Capabilities**
- ✅ **Automatic replica database creation** from blank instances
- ✅ **Dynamic schema and table creation** based on master structure
- ✅ **Intelligent configuration detection** and adaptation
- ✅ **Support for configuration changes** without restart

### **Advanced Replication**
- ✅ **Schema-specific table replication** (configurable via JSON)
- ✅ **Two sync modes**: Full and Incremental
- ✅ **Parallel replica synchronization**
- ✅ **Automatic conflict resolution**
- ✅ **Built-in retry logic and error handling**

### **Production-Ready Features**
- ✅ **Comprehensive logging and monitoring**
- ✅ **Health checks and status reporting**
- ✅ **Cross-platform compatibility** (Docker/Local)
- ✅ **Identity column handling**
- ✅ **Foreign key constraint support**

## 📁 **File Structure**

```
.
├── enhanced_replication_manager.py     # Main enhanced replication engine
├── replication_config_enhanced.json    # Enhanced configuration file
├── setup_master_database.sql          # Master database setup script
├── insert_dummy_data.sql              # Comprehensive dummy data
├── test_enhanced_replication.py       # Enhanced test runner
├── requirements.txt                   # Python dependencies
└── README_Enhanced.md                 # This documentation
```

## ⚙️ **Configuration**

### **Enhanced Configuration File (`replication_config_enhanced.json`)**

```json
{
  "replication": {
    "enabled": true,
    "sync_interval_seconds": 30,
    "auto_setup_replicas": true,        // Auto-create replica databases
    "create_missing_schemas": true,     // Auto-create schemas
    "create_missing_tables": true       // Auto-create tables
  },
  "master_database": {
    "host": "202.4.127.187",
    "port": 3433,
    "username": "sa",
    "password": "YourStrongPassword!123",
    "database": "askd"
  },
  "replica_databases": [
    {
      "name": "alpha-replica",
      "host": "202.4.127.187",
      "port": 3434,
      "username": "sa", 
      "password": "YourStrongPassword!123",
      "database": "askd"
    },
    {
      "name": "beta-replica",
      "host": "202.4.127.187", 
      "port": 3435,
      "username": "sa",
      "password": "YourStrongPassword!123",
      "database": "askd"
    }
  ],
  "schemas_to_replicate": [
    {
      "schema_name": "dbo",
      "tables": [
        {
          "table_name": "Employees",
          "primary_key": "EmployeeID",
          "replicate": true,
          "sync_mode": "incremental",
          "timestamp_column": "ModifiedDate"
        },
        {
          "table_name": "Projects", 
          "primary_key": "ProjectID",
          "replicate": true,
          "sync_mode": "incremental",
          "timestamp_column": "ModifiedDate"
        },
        {
          "table_name": "Tasks",
          "primary_key": "TaskID", 
          "replicate": true,
          "sync_mode": "incremental",
          "timestamp_column": "ModifiedDate"
        }
      ]
    }
  ]
}
```

## 🚀 **Quick Start**

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Setup Master Database**
```bash
# Connect to your master SQL Server and run:
sqlcmd -S 202.4.127.187,3433 -U sa -P "YourStrongPassword!123" -i setup_master_database.sql
```

### **3. Insert Dummy Data (Optional)**
```bash
sqlcmd -S 202.4.127.187,3433 -U sa -P "YourStrongPassword!123" -d askd -i insert_dummy_data.sql
```

### **4. Run Enhanced Replication Manager**

**Option A: Using the test runner (Recommended)**
```bash
python test_enhanced_replication.py
```

**Option B: Direct execution**
```bash
python enhanced_replication_manager.py
```

## 📊 **Database Schema**

### **Tables Created in Master Database:**

#### **`dbo.Employees`**
- **EmployeeID** (PK, Identity)
- FirstName, LastName, Email, Phone
- Department, Position, HireDate, Salary
- ManagerID (FK to Employees), IsActive
- **ModifiedDate** (for incremental sync)

#### **`dbo.Projects`**
- **ProjectID** (PK, Identity)
- ProjectName, Description, StartDate, EndDate
- Budget, Status, ProjectManagerID (FK)
- ClientName, Priority
- **ModifiedDate** (for incremental sync)

#### **`dbo.Tasks`**
- **TaskID** (PK, Identity)
- TaskName, Description, ProjectID (FK)
- AssignedToID (FK), Status, Priority
- EstimatedHours, ActualHours, StartDate, DueDate
- CreatedByID (FK), CompletedDate
- **ModifiedDate** (for incremental sync)

### **Relationships:**
- Employees → Employees (Manager relationship)
- Projects → Employees (Project Manager)
- Tasks → Projects (Project assignment)
- Tasks → Employees (Task assignment & creator)

## 🔄 **How It Works**

### **1. Automatic Setup Process**
1. **Connection Testing**: Validates all database connections
2. **Database Creation**: Creates replica databases if they don't exist
3. **Schema Analysis**: Reads master database schema structure
4. **Schema Creation**: Creates missing schemas in replicas
5. **Table Creation**: Replicates table structure with constraints
6. **Index Creation**: Copies indexes for performance

### **2. Replication Process**
1. **Configuration Loading**: Reads JSON configuration dynamically
2. **Schema Validation**: Ensures replica schemas match master
3. **Data Synchronization**: 
   - **Full Sync**: Complete table copy
   - **Incremental Sync**: Only changed records (based on ModifiedDate)
4. **Parallel Processing**: Syncs multiple replicas simultaneously
5. **Error Handling**: Automatic retry with exponential backoff

### **3. Monitoring and Health Checks**
- Continuous connection monitoring
- Sync performance metrics
- Error logging and reporting
- Configuration change detection

## 🛠 **Advanced Configuration**

### **Adding New Replicas**
Simply add a new replica to the configuration file:

```json
{
  "name": "gamma-replica",
  "host": "202.4.127.187",
  "port": 3436,
  "username": "sa",
  "password": "YourStrongPassword!123", 
  "database": "askd"
}
```

The system will automatically detect and setup the new replica.

### **Adding New Tables**
Add table configuration to replicate new tables:

```json
{
  "table_name": "NewTable",
  "primary_key": "NewTableID",
  "replicate": true,
  "sync_mode": "incremental",
  "timestamp_column": "ModifiedDate"
}
```

### **Sync Mode Options**
- **`"full"`**: Complete table sync every cycle
- **`"incremental"`**: Only sync new/modified records

## 📋 **Testing & Validation**

### **Connection Testing**
```bash
python test_enhanced_replication.py
```

### **Manual Database Verification**
```sql
-- Check replica data
SELECT COUNT(*) FROM dbo.Employees;
SELECT COUNT(*) FROM dbo.Projects; 
SELECT COUNT(*) FROM dbo.Tasks;

-- Verify last sync timestamps
SELECT MAX(ModifiedDate) FROM dbo.Employees;
```

### **Performance Testing**
```sql
-- Insert test data to verify incremental sync
INSERT INTO dbo.Employees (FirstName, LastName, Email, Department) 
VALUES ('Test', 'User', 'test@company.com', 'IT');

-- Check replication after sync interval
```

## 🚨 **Troubleshooting**

### **Common Issues**

1. **Connection Timeouts**
   - Increase `connection_timeout_seconds` in config
   - Check network connectivity
   - Verify SQL Server is accepting connections

2. **Permission Denied**
   - Ensure SA account has proper permissions
   - Check database creation permissions
   - Verify schema creation rights

3. **Identity Insert Errors**
   - System automatically handles identity columns
   - Check for identity column conflicts
   - Review table structure consistency

4. **Sync Performance Issues**
   - Reduce `batch_size` for large tables
   - Increase `sync_interval_seconds`
   - Add indexes on timestamp columns

### **Logging and Monitoring**
```bash
# View real-time logs
tail -f logs/replication_enhanced.log

# Check specific errors
grep "ERROR" logs/replication_enhanced.log
```

## 🔐 **Security Considerations**

- **Password Management**: Use environment variables for production
- **Network Security**: Implement VPN/firewall rules
- **Access Control**: Use dedicated replication service accounts
- **Encryption**: Enable TLS/SSL for database connections

## 🎯 **Production Deployment**

### **Environment Variables**
```bash
export CONFIG_FILE=/path/to/replication_config_enhanced.json
export MASTER_PASSWORD=YourStrongPassword!123
export REPLICA_PASSWORD=YourStrongPassword!123
```

### **Systemd Service (Linux)**
```ini
[Unit]
Description=Enhanced SQL Server Replication Manager
After=network.target

[Service]
Type=simple
User=replication
WorkingDirectory=/opt/replication
ExecStart=/usr/bin/python3 enhanced_replication_manager.py
Restart=always

[Install]
WantedBy=multi-user.target
```

### **Docker Deployment**
```dockerfile
FROM python:3.11-slim
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "enhanced_replication_manager.py"]
```

## 📈 **Performance Optimization**

- **Indexing**: Ensure timestamp columns are indexed
- **Batching**: Adjust batch_size based on available memory
- **Parallel Processing**: System automatically uses thread pools
- **Connection Pooling**: Implemented for efficient resource usage

## 🔄 **Backup and Recovery**

- **Configuration Backup**: Version control your JSON configs
- **Database Backup**: Regular backup of master database
- **Replica Recovery**: System can rebuild replicas from master
- **State Recovery**: Last sync times are persisted

This enhanced system provides enterprise-grade SQL Server replication with automatic setup capabilities, making it perfect for dynamic environments where replica configurations may change frequently.