-- ===============================================
-- Create Data Warehouse with bronze, silver, and gold schemas
-- ===============================================

-- Use master to create the new database
USE master;
GO

-- Create the main data warehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch to the new database context
USE DataWarehouse;
GO

-- Create staging (bronze), cleaned (silver), and analytics-ready (gold) schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
