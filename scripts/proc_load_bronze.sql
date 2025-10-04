/*
=================================================================================
Sored Procedure: Load Bronze Layer (source-> Bronze)
=================================================================================

Script Purpose :
  This stored procedure loads data into the bronze schema from the external CSV files.
It performs the following actions
` TRUNCATES the bronze layer brefore loading the data.
- Uses the 'Bulk INSERT' command to iload data from csv Fils to bronze tables.

Parameters:
NONE
This is a stored procedure does not accept any parameters or return any values.

Usage example :
EXECUTE bronze.load_bronze
====================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 

BEGIN
	DECLARE @start_time AS DATETIME , @end_time AS DATETIME, @bronze_start_time AS DATETIME , @bronze_end_time AS DATETIME 
	BEGIN TRY
		
		SET @bronze_start_time = GETDATE();
		PRINT '====================================================================';
		PRINT 'LOADING BRONZE';
		PRINT '====================================================================';


		PRINT '--------------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '--------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_cust_info';

		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>>Inserting Data Into: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>>TRUNCATING TABLE: bronze.crm_prd_info';

		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>>Inserting Data Into: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK
			);

		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_sales_details';

		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>>Inserting Data Into: bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	 
		PRINT '--------------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '--------------------------------------------------------------------';


		SET @start_time = GETDATE();

		PRINT '>>TRUNCATING TABLE : bronze.erp_cust_az12';

		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>>Inserting Data Into: bronze.erp_cust_az12';

		BULK INSERT bronze.erp_cust_az12

		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK
			);

		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		
		PRINT '>>TRUNCATING TABLE: bronze.erp_loc_a101';

		TRUNCATE TABLE bronze.erp_loc_a101

			PRINT '>>Inserting Data Into: bronze.erp_loc_a101';

		BULK INSERT bronze.erp_loc_a101

		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>>TRUNCATING TABLE: bronze.erp_cat_g1v2';

		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>>Inserting Data Into: bronze.erp_cat_g1v2';

		BULK INSERT bronze.erp_px_cat_g1v2

		FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------';
		SET @bronze_end_time = GETDATE();
		PRINT '================================================================';
		PRINT 'Loading Bronze layer is completed'
		PRINT 'Bronze layer load time:' + CAST(DATEDIFF(second,@bronze_start_time,@bronze_end_time) AS NVARCHAR) + 'seconds';
		PRINT '================================================================';
	END TRY
	BEGIN CATCH
	PRINT '====================================================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE';
	PRINT 'ERROR Message' + ERROR_MESSAGE();
	PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR State' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '====================================================================';
	END CATCH
END
