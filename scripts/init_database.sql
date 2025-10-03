/*
==================================================================
Create Database and Schemas
==================================================================
Script purpose :
  This script creates a new database named 'DataWarehouse' after checking if it already exists .
  If the database exists , it is dropped and recreated. 
  ADditionally the script sets up three schemas within the database : 'bronze' , 'silver' , and 'gold'

 Drop and recreate the 'Data Warehouse if it exists.
  All data in the database will be deleted permamnently if it exists. Proceed with caution
  and ensure you have proper backups before running this script
 */
  
USE master;
GO

IF EXISTS (SELECT 1 FROM sys.database WHERE name= 'DataWarehouse')
  BEGIN 
    ALTER DATABASE DataWarehouse SET SINGEL_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
  
-- Create the database  'Data Warehouse'
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create schema
CREATE SCHEMA bronze;

GO

CREATE SCHEMA silver;

GO

CREATE SCHEMA gold;


