-- Insert Dummy Data Script for Master Database
-- This script populates the master database with realistic test data

USE askd;
GO

-- Clear existing data (if any) in proper order due to foreign key constraints
DELETE FROM dbo.Tasks;
DELETE FROM dbo.Projects;
DELETE FROM dbo.Employees;
GO

-- Reset identity seeds
DBCC CHECKIDENT ('dbo.Tasks', RESEED, 0);
DBCC CHECKIDENT ('dbo.Projects', RESEED, 0);
DBCC CHECKIDENT ('dbo.Employees', RESEED, 0);
GO

PRINT 'Inserting dummy data...';

-- Insert Employees (insert managers first, then regular employees)
INSERT INTO dbo.Employees (FirstName, LastName, Email, Phone, Department, Position, HireDate, Salary, IsActive, ManagerID) VALUES
-- Senior Management (no manager)
('John', 'Smith', 'john.smith@company.com', '+1-555-0101', 'Executive', 'CEO', '2020-01-15', 150000.00, 1, NULL),
('Sarah', 'Johnson', 'sarah.johnson@company.com', '+1-555-0102', 'Executive', 'CTO', '2020-02-01', 140000.00, 1, 1),
('Michael', 'Brown', 'michael.brown@company.com', '+1-555-0103', 'Executive', 'CFO', '2020-03-15', 135000.00, 1, 1),

-- IT Department
('David', 'Wilson', 'david.wilson@company.com', '+1-555-0201', 'IT', 'IT Director', '2020-05-01', 120000.00, 1, 2),
('Emily', 'Davis', 'emily.davis@company.com', '+1-555-0202', 'IT', 'Senior Developer', '2020-07-15', 95000.00, 1, 4),
('James', 'Miller', 'james.miller@company.com', '+1-555-0203', 'IT', 'DevOps Engineer', '2021-01-10', 90000.00, 1, 4),
('Lisa', 'Anderson', 'lisa.anderson@company.com', '+1-555-0204', 'IT', 'QA Manager', '2021-03-20', 85000.00, 1, 4),
('Robert', 'Taylor', 'robert.taylor@company.com', '+1-555-0205', 'IT', 'Junior Developer', '2022-06-01', 70000.00, 1, 5),
('Jessica', 'White', 'jessica.white@company.com', '+1-555-0206', 'IT', 'System Administrator', '2022-08-15', 75000.00, 1, 4),
('Daniel', 'Thomas', 'daniel.thomas@company.com', '+1-555-0207', 'IT', 'Database Administrator', '2021-11-30', 80000.00, 1, 4),

-- Marketing Department
('Amanda', 'Garcia', 'amanda.garcia@company.com', '+1-555-0301', 'Marketing', 'Marketing Director', '2020-04-01', 110000.00, 1, 1),
('Christopher', 'Martinez', 'christopher.martinez@company.com', '+1-555-0302', 'Marketing', 'Digital Marketing Specialist', '2021-05-15', 65000.00, 1, 11),
('Michelle', 'Rodriguez', 'michelle.rodriguez@company.com', '+1-555-0303', 'Marketing', 'Content Manager', '2021-07-01', 60000.00, 1, 11),
('Kevin', 'Lee', 'kevin.lee@company.com', '+1-555-0304', 'Marketing', 'Social Media Manager', '2022-01-15', 55000.00, 1, 11),

-- Sales Department
('Rachel', 'Clark', 'rachel.clark@company.com', '+1-555-0401', 'Sales', 'Sales Director', '2020-06-01', 115000.00, 1, 1),
('Thomas', 'Lewis', 'thomas.lewis@company.com', '+1-555-0402', 'Sales', 'Senior Sales Representative', '2021-02-01', 75000.00, 1, 15),
('Nicole', 'Hall', 'nicole.hall@company.com', '+1-555-0403', 'Sales', 'Sales Representative', '2021-09-15', 60000.00, 1, 15),
('Brian', 'Young', 'brian.young@company.com', '+1-555-0404', 'Sales', 'Sales Representative', '2022-03-01', 58000.00, 1, 15),

-- HR Department
('Jennifer', 'King', 'jennifer.king@company.com', '+1-555-0501', 'HR', 'HR Director', '2020-08-01', 105000.00, 1, 1),
('Mark', 'Wright', 'mark.wright@company.com', '+1-555-0502', 'HR', 'HR Specialist', '2021-10-01', 65000.00, 1, 19),
('Laura', 'Green', 'laura.green@company.com', '+1-555-0503', 'HR', 'Recruiter', '2022-02-15', 55000.00, 1, 19);

PRINT 'Employees inserted successfully.';

-- Insert Projects
INSERT INTO dbo.Projects (ProjectName, Description, StartDate, EndDate, Budget, Status, ProjectManagerID, ClientName, Priority) VALUES
('Customer Portal Redesign', 'Complete redesign of customer-facing web portal with modern UI/UX', '2023-01-15', '2023-06-30', 250000.00, 'Active', 5, 'Internal Project', 'High'),
('Mobile App Development', 'Development of iOS and Android mobile application for customer engagement', '2023-02-01', '2023-08-15', 180000.00, 'Active', 5, 'TechCorp Inc', 'High'),
('Data Migration Project', 'Migration of legacy data systems to new cloud infrastructure', '2023-03-01', '2023-05-31', 120000.00, 'Completed', 10, 'DataSolutions Ltd', 'Medium'),
('Marketing Automation Platform', 'Implementation of new marketing automation and CRM system', '2023-04-01', '2023-07-31', 95000.00, 'Active', 12, 'MarketPro Agency', 'Medium'),
('Security Audit and Compliance', 'Comprehensive security audit and compliance certification process', '2023-05-01', '2023-09-30', 75000.00, 'Planning', 6, 'SecureIT Consulting', 'Critical'),
('E-commerce Platform Upgrade', 'Upgrade existing e-commerce platform with new features and performance improvements', '2023-06-01', '2023-10-15', 200000.00, 'Planning', 5, 'RetailMax Corp', 'High'),
('Business Intelligence Dashboard', 'Development of executive dashboard for business intelligence and reporting', '2023-07-01', '2023-11-30', 150000.00, 'Planning', 10, 'Internal Project', 'Medium'),
('Cloud Infrastructure Migration', 'Migration of on-premises infrastructure to AWS cloud services', '2023-01-01', '2023-04-30', 300000.00, 'Completed', 6, 'CloudFirst Solutions', 'Critical');

PRINT 'Projects inserted successfully.';

-- Insert Tasks
INSERT INTO dbo.Tasks (TaskName, Description, ProjectID, AssignedToID, Status, Priority, EstimatedHours, ActualHours, StartDate, DueDate, CompletedDate, CreatedByID) VALUES
-- Customer Portal Redesign Tasks
('Requirements Gathering', 'Collect and document functional and non-functional requirements', 1, 5, 'Done', 'High', 40.0, 42.0, '2023-01-15', '2023-01-25', '2023-01-24', 5),
('UI/UX Design', 'Create wireframes, mockups, and user interface designs', 1, 12, 'Done', 'High', 60.0, 58.0, '2023-01-26', '2023-02-15', '2023-02-14', 5),
('Frontend Development', 'Develop responsive frontend using React and modern CSS', 1, 8, 'In Progress', 'High', 120.0, 75.0, '2023-02-16', '2023-04-15', NULL, 5),
('Backend API Development', 'Develop RESTful APIs for customer portal functionality', 1, 5, 'In Progress', 'High', 100.0, 65.0, '2023-02-20', '2023-04-30', NULL, 5),
('Database Schema Design', 'Design and implement optimized database schema', 1, 10, 'Done', 'Medium', 30.0, 28.0, '2023-02-01', '2023-02-10', '2023-02-09', 5),
('Integration Testing', 'Perform comprehensive integration testing', 1, 7, 'To Do', 'Medium', 50.0, 0.0, '2023-05-01', '2023-05-15', NULL, 7),

-- Mobile App Development Tasks
('Platform Research', 'Research and select appropriate mobile development frameworks', 2, 5, 'Done', 'High', 20.0, 18.0, '2023-02-01', '2023-02-07', '2023-02-06', 5),
('App Architecture Design', 'Design scalable architecture for mobile applications', 2, 5, 'Done', 'High', 40.0, 45.0, '2023-02-08', '2023-02-20', '2023-02-19', 5),
('iOS Development', 'Develop native iOS application', 2, 8, 'In Progress', 'High', 150.0, 90.0, '2023-02-21', '2023-06-15', NULL, 5),
('Android Development', 'Develop native Android application', 2, 8, 'In Progress', 'High', 150.0, 85.0, '2023-02-21', '2023-06-15', NULL, 5),
('API Integration', 'Integrate mobile apps with backend APIs', 2, 5, 'To Do', 'High', 60.0, 0.0, '2023-06-01', '2023-06-30', NULL, 5),
('App Store Submission', 'Prepare and submit apps to App Store and Google Play', 2, 5, 'To Do', 'Medium', 20.0, 0.0, '2023-07-15', '2023-08-01', NULL, 5),

-- Data Migration Project Tasks
('Data Audit', 'Audit existing data systems and identify migration requirements', 3, 10, 'Done', 'Critical', 50.0, 52.0, '2023-03-01', '2023-03-10', '2023-03-09', 10),
('Migration Strategy', 'Develop comprehensive data migration strategy and timeline', 3, 10, 'Done', 'Critical', 30.0, 35.0, '2023-03-11', '2023-03-20', '2023-03-18', 10),
('ETL Development', 'Develop Extract, Transform, Load processes for data migration', 3, 6, 'Done', 'High', 80.0, 85.0, '2023-03-21', '2023-04-15', '2023-04-14', 10),
('Data Validation', 'Validate migrated data for accuracy and completeness', 3, 7, 'Done', 'Critical', 40.0, 38.0, '2023-04-16', '2023-05-01', '2023-04-30', 10),
('Go-Live Support', 'Provide support during production migration go-live', 3, 10, 'Done', 'Critical', 24.0, 26.0, '2023-05-15', '2023-05-31', '2023-05-30', 10),

-- Marketing Automation Platform Tasks
('Vendor Evaluation', 'Evaluate and select marketing automation platform vendor', 4, 12, 'Done', 'High', 30.0, 32.0, '2023-04-01', '2023-04-15', '2023-04-14', 12),
('System Configuration', 'Configure marketing automation platform for company needs', 4, 12, 'In Progress', 'High', 60.0, 35.0, '2023-04-16', '2023-05-31', NULL, 12),
('Data Import', 'Import existing customer and lead data into new system', 4, 13, 'To Do', 'Medium', 25.0, 0.0, '2023-06-01', '2023-06-15', NULL, 12),
('Campaign Setup', 'Set up initial marketing campaigns and workflows', 4, 14, 'To Do', 'Medium', 40.0, 0.0, '2023-06-16', '2023-07-15', NULL, 12),
('Training Sessions', 'Conduct training sessions for marketing team', 4, 12, 'To Do', 'Low', 16.0, 0.0, '2023-07-16', '2023-07-31', NULL, 12),

-- Security Audit Tasks
('Security Assessment', 'Conduct comprehensive security assessment of current systems', 5, 6, 'To Do', 'Critical', 60.0, 0.0, '2023-05-01', '2023-05-31', NULL, 6),
('Vulnerability Testing', 'Perform penetration testing and vulnerability assessment', 5, 9, 'To Do', 'Critical', 40.0, 0.0, '2023-06-01', '2023-06-30', NULL, 6),
('Compliance Documentation', 'Prepare documentation for compliance certification', 5, 6, 'To Do', 'High', 50.0, 0.0, '2023-07-01', '2023-08-15', NULL, 6),
('Security Policy Updates', 'Update company security policies and procedures', 5, 19, 'To Do', 'Medium', 30.0, 0.0, '2023-08-01', '2023-09-01', NULL, 6),

-- Add more tasks for other projects
('Requirements Analysis', 'Analyze requirements for e-commerce platform upgrade', 6, 5, 'To Do', 'High', 35.0, 0.0, '2023-06-01', '2023-06-15', NULL, 5),
('Performance Optimization', 'Optimize platform performance and scalability', 6, 6, 'To Do', 'High', 80.0, 0.0, '2023-07-01', '2023-08-31', NULL, 5),
('Payment Gateway Integration', 'Integrate new payment gateway solutions', 6, 8, 'To Do', 'High', 45.0, 0.0, '2023-08-01', '2023-09-15', NULL, 5),

('Dashboard Design', 'Design executive dashboard interface and user experience', 7, 12, 'To Do', 'Medium', 40.0, 0.0, '2023-07-01', '2023-07-31', NULL, 10),
('Data Warehouse Setup', 'Set up data warehouse for business intelligence', 7, 10, 'To Do', 'High', 60.0, 0.0, '2023-08-01', '2023-09-15', NULL, 10),
('Report Development', 'Develop automated reporting capabilities', 7, 5, 'To Do', 'Medium', 50.0, 0.0, '2023-09-01', '2023-10-31', NULL, 10),

('Cloud Architecture Planning', 'Plan and design cloud infrastructure architecture', 8, 6, 'Done', 'Critical', 50.0, 55.0, '2023-01-01', '2023-01-20', '2023-01-18', 6),
('Migration Execution', 'Execute migration of services to cloud infrastructure', 8, 9, 'Done', 'Critical', 120.0, 125.0, '2023-02-01', '2023-04-15', '2023-04-12', 6),
('Performance Testing', 'Test performance of migrated cloud infrastructure', 8, 7, 'Done', 'High', 30.0, 32.0, '2023-04-16', '2023-04-30', '2023-04-28', 6);

PRINT 'Tasks inserted successfully.';

-- Update some records to test incremental replication
UPDATE dbo.Employees SET Salary = Salary * 1.05 WHERE Department = 'IT';
UPDATE dbo.Projects SET Status = 'On Hold' WHERE ProjectID = 5;
UPDATE dbo.Tasks SET Status = 'Review' WHERE TaskID IN (3, 4);

PRINT 'Sample data updates applied.';

-- Display summary statistics
SELECT 
    'Summary Statistics' as Info,
    (SELECT COUNT(*) FROM dbo.Employees) as TotalEmployees,
    (SELECT COUNT(*) FROM dbo.Projects) as TotalProjects,
    (SELECT COUNT(*) FROM dbo.Tasks) as TotalTasks;

SELECT 'Employee Count by Department' as Info, Department, COUNT(*) as Count
FROM dbo.Employees 
WHERE IsActive = 1
GROUP BY Department
ORDER BY Count DESC;

SELECT 'Project Status Distribution' as Info, Status, COUNT(*) as Count
FROM dbo.Projects
GROUP BY Status
ORDER BY Count DESC;

SELECT 'Task Status Distribution' as Info, Status, COUNT(*) as Count
FROM dbo.Tasks
GROUP BY Status
ORDER BY Count DESC;

PRINT 'Dummy data insertion completed successfully!';
GO