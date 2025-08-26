-- Prerequisites Setup Script for Master Database
-- Run this script BEFORE executing the raw_replication_manager.py
-- This creates the required database structure for replication

USE master;
GO

-- Step 1: Create the master database (askd)
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'askd')
BEGIN
    PRINT 'Creating database [askd]...';
    CREATE DATABASE [askd];
    PRINT 'Database [askd] created successfully.';
END
ELSE
BEGIN
    PRINT 'Database [askd] already exists.';
END
GO

-- Switch to the askd database
USE [askd];
GO

-- Step 2: Create the dbo schema (usually exists by default, but ensure it's there)
IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = 'dbo')
BEGIN
    PRINT 'Creating schema [dbo]...';
    EXEC('CREATE SCHEMA [dbo]');
    PRINT 'Schema [dbo] created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema [dbo] already exists.';
END
GO

-- Step 3: Create the required tables based on your replication_config_enhanced.json

-- Create Employees table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Employees')
BEGIN
    PRINT 'Creating table [dbo].[Employees]...';
    CREATE TABLE [dbo].[Employees] (
        [EmployeeID] int IDENTITY(1,1) NOT NULL,
        [FirstName] nvarchar(50) NOT NULL,
        [LastName] nvarchar(50) NOT NULL,
        [Email] nvarchar(255) NOT NULL,
        [Phone] nvarchar(20) NULL,
        [Department] nvarchar(50) NOT NULL,
        [Position] nvarchar(100) NOT NULL,
        [HireDate] datetime2 NOT NULL DEFAULT GETDATE(),
        [Salary] decimal(18,2) NULL,
        [IsActive] bit NOT NULL DEFAULT 1,
        [ManagerID] int NULL,
        [ModifiedDate] datetime2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT [PK_Employees] PRIMARY KEY ([EmployeeID]),
        CONSTRAINT [FK_Employees_Manager] FOREIGN KEY ([ManagerID]) REFERENCES [dbo].[Employees]([EmployeeID]),
        CONSTRAINT [UK_Employees_Email] UNIQUE ([Email])
    );
    
    -- Create trigger to update ModifiedDate
    CREATE TRIGGER [tr_Employees_UpdateModifiedDate]
    ON [dbo].[Employees]
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE [dbo].[Employees]
        SET [ModifiedDate] = GETDATE()
        WHERE [EmployeeID] IN (SELECT [EmployeeID] FROM inserted);
    END;
    
    PRINT 'Table [dbo].[Employees] created successfully with trigger.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Employees] already exists.';
END
GO

-- Create Projects table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Projects')
BEGIN
    PRINT 'Creating table [dbo].[Projects]...';
    CREATE TABLE [dbo].[Projects] (
        [ProjectID] int IDENTITY(1,1) NOT NULL,
        [ProjectName] nvarchar(255) NOT NULL,
        [Description] nvarchar(1000) NULL,
        [StartDate] datetime2 NOT NULL,
        [EndDate] datetime2 NULL,
        [Budget] decimal(18,2) NULL,
        [Status] nvarchar(50) NOT NULL DEFAULT 'Planning',
        [ProjectManagerID] int NULL,
        [ClientName] nvarchar(255) NULL,
        [Priority] nvarchar(20) NOT NULL DEFAULT 'Medium',
        [ModifiedDate] datetime2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT [PK_Projects] PRIMARY KEY ([ProjectID]),
        CONSTRAINT [FK_Projects_ProjectManager] FOREIGN KEY ([ProjectManagerID]) REFERENCES [dbo].[Employees]([EmployeeID]),
        CONSTRAINT [CK_Projects_Status] CHECK ([Status] IN ('Planning', 'Active', 'On Hold', 'Completed', 'Cancelled')),
        CONSTRAINT [CK_Projects_Priority] CHECK ([Priority] IN ('Low', 'Medium', 'High', 'Critical'))
    );
    
    -- Create trigger to update ModifiedDate
    CREATE TRIGGER [tr_Projects_UpdateModifiedDate]
    ON [dbo].[Projects]
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE [dbo].[Projects]
        SET [ModifiedDate] = GETDATE()
        WHERE [ProjectID] IN (SELECT [ProjectID] FROM inserted);
    END;
    
    PRINT 'Table [dbo].[Projects] created successfully with trigger.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Projects] already exists.';
END
GO

-- Create Tasks table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Tasks')
BEGIN
    PRINT 'Creating table [dbo].[Tasks]...';
    CREATE TABLE [dbo].[Tasks] (
        [TaskID] int IDENTITY(1,1) NOT NULL,
        [TaskName] nvarchar(255) NOT NULL,
        [Description] nvarchar(1000) NULL,
        [ProjectID] int NOT NULL,
        [AssignedToID] int NULL,
        [Status] nvarchar(50) NOT NULL DEFAULT 'To Do',
        [Priority] nvarchar(20) NOT NULL DEFAULT 'Medium',
        [EstimatedHours] decimal(8,2) NULL,
        [ActualHours] decimal(8,2) NULL,
        [StartDate] datetime2 NULL,
        [DueDate] datetime2 NULL,
        [CompletedDate] datetime2 NULL,
        [CreatedByID] int NOT NULL,
        [ModifiedDate] datetime2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT [PK_Tasks] PRIMARY KEY ([TaskID]),
        CONSTRAINT [FK_Tasks_Project] FOREIGN KEY ([ProjectID]) REFERENCES [dbo].[Projects]([ProjectID]),
        CONSTRAINT [FK_Tasks_AssignedTo] FOREIGN KEY ([AssignedToID]) REFERENCES [dbo].[Employees]([EmployeeID]),
        CONSTRAINT [FK_Tasks_CreatedBy] FOREIGN KEY ([CreatedByID]) REFERENCES [dbo].[Employees]([EmployeeID]),
        CONSTRAINT [CK_Tasks_Status] CHECK ([Status] IN ('To Do', 'In Progress', 'Review', 'Done', 'Cancelled')),
        CONSTRAINT [CK_Tasks_Priority] CHECK ([Priority] IN ('Low', 'Medium', 'High', 'Critical'))
    );
    
    -- Create trigger to update ModifiedDate
    CREATE TRIGGER [tr_Tasks_UpdateModifiedDate]
    ON [dbo].[Tasks]
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE [dbo].[Tasks]
        SET [ModifiedDate] = GETDATE()
        WHERE [TaskID] IN (SELECT [TaskID] FROM inserted);
    END;
    
    PRINT 'Table [dbo].[Tasks] created successfully with trigger.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Tasks] already exists.';
END
GO

-- Step 4: Create useful indexes for performance
PRINT 'Creating performance indexes...';

-- Employees indexes
IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Employees_Department' AND object_id = OBJECT_ID('[dbo].[Employees]'))
    CREATE INDEX [IX_Employees_Department] ON [dbo].[Employees] ([Department]);

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Employees_ManagerID' AND object_id = OBJECT_ID('[dbo].[Employees]'))
    CREATE INDEX [IX_Employees_ManagerID] ON [dbo].[Employees] ([ManagerID]);

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Employees_ModifiedDate' AND object_id = OBJECT_ID('[dbo].[Employees]'))
    CREATE INDEX [IX_Employees_ModifiedDate] ON [dbo].[Employees] ([ModifiedDate]);

-- Projects indexes
IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Projects_Status' AND object_id = OBJECT_ID('[dbo].[Projects]'))
    CREATE INDEX [IX_Projects_Status] ON [dbo].[Projects] ([Status]);

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Projects_ProjectManagerID' AND object_id = OBJECT_ID('[dbo].[Projects]'))
    CREATE INDEX [IX_Projects_ProjectManagerID] ON [dbo].[Projects] ([ProjectManagerID]);

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Projects_ModifiedDate' AND object_id = OBJECT_ID('[dbo].[Projects]'))
    CREATE INDEX [IX_Projects_ModifiedDate] ON [dbo].[Projects] ([ModifiedDate]);

-- Tasks indexes
IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Tasks_ProjectID' AND object_id = OBJECT_ID('[dbo].[Tasks]'))
    CREATE INDEX [IX_Tasks_ProjectID] ON [dbo].[Tasks] ([ProjectID]);

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Tasks_AssignedToID' AND object_id = OBJECT_ID('[dbo].[Tasks]'))
    CREATE INDEX [IX_Tasks_AssignedToID] ON [dbo].[Tasks] ([AssignedToID]);

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Tasks_Status' AND object_id = OBJECT_ID('[dbo].[Tasks]'))
    CREATE INDEX [IX_Tasks_Status] ON [dbo].[Tasks] ([Status]);

IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_Tasks_ModifiedDate' AND object_id = OBJECT_ID('[dbo].[Tasks]'))
    CREATE INDEX [IX_Tasks_ModifiedDate] ON [dbo].[Tasks] ([ModifiedDate]);

PRINT 'Performance indexes created successfully.';
GO

-- Step 5: Insert sample data for testing replication
PRINT 'Inserting sample data...';

-- Insert sample employees (managers first)
SET IDENTITY_INSERT [dbo].[Employees] ON;

INSERT INTO [dbo].[Employees] ([EmployeeID], [FirstName], [LastName], [Email], [Phone], [Department], [Position], [HireDate], [Salary], [IsActive], [ManagerID])
VALUES 
    (1, 'John', 'Smith', 'john.smith@company.com', '+1-555-0101', 'Executive', 'CEO', '2020-01-15', 150000.00, 1, NULL),
    (2, 'Sarah', 'Johnson', 'sarah.johnson@company.com', '+1-555-0102', 'IT', 'CTO', '2020-02-01', 140000.00, 1, 1),
    (3, 'Michael', 'Brown', 'michael.brown@company.com', '+1-555-0103', 'IT', 'Senior Developer', '2020-03-15', 95000.00, 1, 2),
    (4, 'Emily', 'Davis', 'emily.davis@company.com', '+1-555-0104', 'IT', 'Junior Developer', '2021-01-10', 70000.00, 1, 3),
    (5, 'David', 'Wilson', 'david.wilson@company.com', '+1-555-0105', 'Marketing', 'Marketing Manager', '2020-05-01', 85000.00, 1, 1);

SET IDENTITY_INSERT [dbo].[Employees] OFF;

-- Insert sample projects
SET IDENTITY_INSERT [dbo].[Projects] ON;

INSERT INTO [dbo].[Projects] ([ProjectID], [ProjectName], [Description], [StartDate], [EndDate], [Budget], [Status], [ProjectManagerID], [ClientName], [Priority])
VALUES 
    (1, 'Customer Portal Redesign', 'Complete redesign of customer-facing web portal', '2023-01-15', '2023-06-30', 250000.00, 'Active', 2, 'Internal Project', 'High'),
    (2, 'Mobile App Development', 'Development of mobile application for customer engagement', '2023-02-01', '2023-08-15', 180000.00, 'Active', 3, 'TechCorp Inc', 'High'),
    (3, 'Marketing Campaign', 'Q2 marketing campaign for new product launch', '2023-04-01', '2023-07-31', 95000.00, 'Planning', 5, 'MarketPro Agency', 'Medium');

SET IDENTITY_INSERT [dbo].[Projects] OFF;

-- Insert sample tasks
SET IDENTITY_INSERT [dbo].[Tasks] ON;

INSERT INTO [dbo].[Tasks] ([TaskID], [TaskName], [Description], [ProjectID], [AssignedToID], [Status], [Priority], [EstimatedHours], [ActualHours], [StartDate], [DueDate], [CreatedByID])
VALUES 
    (1, 'Requirements Gathering', 'Collect and document functional requirements', 1, 3, 'Done', 'High', 40.0, 42.0, '2023-01-15', '2023-01-25', 2),
    (2, 'Frontend Development', 'Develop responsive frontend components', 1, 4, 'In Progress', 'High', 120.0, 75.0, '2023-02-16', '2023-04-15', 2),
    (3, 'Backend API Development', 'Develop RESTful APIs', 1, 3, 'In Progress', 'High', 100.0, 65.0, '2023-02-20', '2023-04-30', 2),
    (4, 'Mobile App Architecture', 'Design app architecture', 2, 3, 'To Do', 'High', 50.0, 0.0, '2023-02-01', '2023-02-15', 2),
    (5, 'Campaign Strategy', 'Develop marketing campaign strategy', 3, 5, 'To Do', 'Medium', 30.0, 0.0, '2023-04-01', '2023-04-15', 5);

SET IDENTITY_INSERT [dbo].[Tasks] OFF;

PRINT 'Sample data inserted successfully.';
GO

-- Step 6: Enable necessary SQL Server features for replication
PRINT 'Configuring SQL Server for replication...';

-- Enable SQL Server Agent (required for replication)
-- Note: This may require manual enabling if SQL Server Agent is not running

-- Check if SQL Server Agent is running
DECLARE @agent_status int;
EXEC master.dbo.xp_servicecontrol 'QueryState', N'SQLServerAgent';

-- Display current database configuration
SELECT 
    name AS DatabaseName,
    database_id,
    is_published,
    is_subscribed,
    is_distributor,
    is_published + is_subscribed + is_distributor AS ReplicationFlags
FROM sys.databases 
WHERE name = 'askd';

PRINT 'Database structure setup completed successfully!';
PRINT '';
PRINT '=== NEXT STEPS ===';
PRINT '1. Ensure SQL Server Agent is running on all instances';
PRINT '2. Verify network connectivity between master and replica servers';
PRINT '3. Confirm login credentials in replication_config_enhanced.json';
PRINT '4. Run: python test_raw_replication.py (to validate setup)';
PRINT '5. Run: python raw_replication_manager.py (to start replication)';
PRINT '';
PRINT 'Prerequisites setup completed! ✅';
GO