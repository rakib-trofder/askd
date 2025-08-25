-- Enhanced Master Database Setup Script
-- This script sets up the master database with the required schema and tables

USE master;
GO

-- Create the database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'askd')
BEGIN
    CREATE DATABASE askd;
    PRINT 'Database [askd] created successfully.';
END
ELSE
BEGIN
    PRINT 'Database [askd] already exists.';
END
GO

USE askd;
GO

-- Enable SQL Server Agent (required for some replication features)
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
GO

-- Create Employees table in dbo schema
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Employees')
BEGIN
    CREATE TABLE dbo.Employees (
        EmployeeID int IDENTITY(1,1) PRIMARY KEY,
        FirstName nvarchar(50) NOT NULL,
        LastName nvarchar(50) NOT NULL,
        Email nvarchar(100) UNIQUE,
        Phone nvarchar(20),
        Department nvarchar(50),
        Position nvarchar(100),
        HireDate datetime2 DEFAULT GETDATE(),
        Salary decimal(10,2),
        IsActive bit DEFAULT 1,
        ManagerID int,
        CreatedDate datetime2 DEFAULT GETDATE(),
        ModifiedDate datetime2 DEFAULT GETDATE(),
        
        CONSTRAINT FK_Employees_Manager FOREIGN KEY (ManagerID) REFERENCES dbo.Employees(EmployeeID)
    );
    PRINT 'Table [dbo].[Employees] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Employees] already exists.';
END
GO

-- Create Projects table in dbo schema
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Projects')
BEGIN
    CREATE TABLE dbo.Projects (
        ProjectID int IDENTITY(1,1) PRIMARY KEY,
        ProjectName nvarchar(100) NOT NULL,
        Description nvarchar(500),
        StartDate datetime2,
        EndDate datetime2,
        Budget decimal(15,2),
        Status nvarchar(50) DEFAULT 'Planning',
        ProjectManagerID int,
        ClientName nvarchar(100),
        Priority nvarchar(20) DEFAULT 'Medium',
        CreatedDate datetime2 DEFAULT GETDATE(),
        ModifiedDate datetime2 DEFAULT GETDATE(),
        
        CONSTRAINT FK_Projects_Manager FOREIGN KEY (ProjectManagerID) REFERENCES dbo.Employees(EmployeeID),
        CONSTRAINT CHK_Projects_Status CHECK (Status IN ('Planning', 'Active', 'On Hold', 'Completed', 'Cancelled')),
        CONSTRAINT CHK_Projects_Priority CHECK (Priority IN ('Low', 'Medium', 'High', 'Critical'))
    );
    PRINT 'Table [dbo].[Projects] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Projects] already exists.';
END
GO

-- Create Tasks table in dbo schema
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Tasks')
BEGIN
    CREATE TABLE dbo.Tasks (
        TaskID int IDENTITY(1,1) PRIMARY KEY,
        TaskName nvarchar(200) NOT NULL,
        Description nvarchar(1000),
        ProjectID int NOT NULL,
        AssignedToID int,
        Status nvarchar(50) DEFAULT 'To Do',
        Priority nvarchar(20) DEFAULT 'Medium',
        EstimatedHours decimal(5,2),
        ActualHours decimal(5,2),
        StartDate datetime2,
        DueDate datetime2,
        CompletedDate datetime2,
        CreatedByID int,
        CreatedDate datetime2 DEFAULT GETDATE(),
        ModifiedDate datetime2 DEFAULT GETDATE(),
        
        CONSTRAINT FK_Tasks_Project FOREIGN KEY (ProjectID) REFERENCES dbo.Projects(ProjectID),
        CONSTRAINT FK_Tasks_AssignedTo FOREIGN KEY (AssignedToID) REFERENCES dbo.Employees(EmployeeID),
        CONSTRAINT FK_Tasks_CreatedBy FOREIGN KEY (CreatedByID) REFERENCES dbo.Employees(EmployeeID),
        CONSTRAINT CHK_Tasks_Status CHECK (Status IN ('To Do', 'In Progress', 'Review', 'Testing', 'Done', 'Blocked')),
        CONSTRAINT CHK_Tasks_Priority CHECK (Priority IN ('Low', 'Medium', 'High', 'Critical'))
    );
    PRINT 'Table [dbo].[Tasks] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Tasks] already exists.';
END
GO

-- Create indexes for better performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Employees') AND name = 'IX_Employees_Department')
BEGIN
    CREATE INDEX IX_Employees_Department ON dbo.Employees(Department);
    PRINT 'Index IX_Employees_Department created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Projects') AND name = 'IX_Projects_Status')
BEGIN
    CREATE INDEX IX_Projects_Status ON dbo.Projects(Status);
    PRINT 'Index IX_Projects_Status created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Tasks') AND name = 'IX_Tasks_ProjectID')
BEGIN
    CREATE INDEX IX_Tasks_ProjectID ON dbo.Tasks(ProjectID);
    PRINT 'Index IX_Tasks_ProjectID created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Tasks') AND name = 'IX_Tasks_AssignedToID')
BEGIN
    CREATE INDEX IX_Tasks_AssignedToID ON dbo.Tasks(AssignedToID);
    PRINT 'Index IX_Tasks_AssignedToID created.';
END
GO

-- Create triggers to update ModifiedDate automatically
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID('dbo.TR_Employees_UpdateModifiedDate'))
BEGIN
    EXEC('
    CREATE TRIGGER TR_Employees_UpdateModifiedDate
    ON dbo.Employees
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE dbo.Employees 
        SET ModifiedDate = GETDATE() 
        WHERE EmployeeID IN (SELECT EmployeeID FROM inserted);
    END
    ');
    PRINT 'Trigger TR_Employees_UpdateModifiedDate created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID('dbo.TR_Projects_UpdateModifiedDate'))
BEGIN
    EXEC('
    CREATE TRIGGER TR_Projects_UpdateModifiedDate
    ON dbo.Projects
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE dbo.Projects 
        SET ModifiedDate = GETDATE() 
        WHERE ProjectID IN (SELECT ProjectID FROM inserted);
    END
    ');
    PRINT 'Trigger TR_Projects_UpdateModifiedDate created.';
END
GO

IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID('dbo.TR_Tasks_UpdateModifiedDate'))
BEGIN
    EXEC('
    CREATE TRIGGER TR_Tasks_UpdateModifiedDate
    ON dbo.Tasks
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE dbo.Tasks 
        SET ModifiedDate = GETDATE() 
        WHERE TaskID IN (SELECT TaskID FROM inserted);
    END
    ');
    PRINT 'Trigger TR_Tasks_UpdateModifiedDate created.';
END
GO

PRINT 'Master database setup completed successfully.';
GO