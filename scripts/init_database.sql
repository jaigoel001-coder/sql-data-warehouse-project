/*
=====================================================
CREATE DATABASE AND SCHEMAS
=====================================================
Script Purpose:
This script creates a new database names 'DataWarehouse' after checking if it already exists.
If the database exists, it is droped and recreated. Additionally, the script setup 3 schemas
within the database: 'Bronze', 'Silver' & 'Gold'

WARNING:
Running this script will drop the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted. Procced with caution
and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' Database
IF EXISTS (SELECT 1 FROM sys.databases WHERE Name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO

-- Create New Database 'DATAWAREHOUSE'
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- STEP-2: CREATE SCHEMAS

CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
