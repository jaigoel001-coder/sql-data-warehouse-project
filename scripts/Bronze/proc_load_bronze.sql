/*
=====================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================================
Script Purpose:
      This Stored Procedure loads data into 'Bronze' schema from external CSV files.
      It perform the following actions:
      - Truncates the bronze tables before loading data
      - Uses the 'Bulk Insert' command to load data from CSV files to bronze tables.

Parameters:
      NONE
      This Stored Procedure does not accept any parameters or return and values.

Usage Example:
      EXEC Bronze.load_procedure;
==================================================================================
*/

CREATE OR ALTER PROCEDURE Bronze.load_procedure AS
BEGIN
	DECLARE @Start_time DATETIME, @End_time DATETIME, @Batch_start_time DATETIME, @Batch_end_time DATETIME;
	BEGIN TRY
		PRINT '===================================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===================================================================================';

		PRINT '-----------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------------------------';

		SET @Batch_start_time = GETDATE();
		-- Table 1 SOURCE DATA CRM
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.crm_cust_info';
		TRUNCATE TABLE Bronze.crm_cust_info;

		PRINT '>> Inserting Data into: Bronze.crm_cust_info';
		BULK INSERT Bronze.crm_cust_info
		FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH ( FIRSTROW = 2,
			   FIELDTERMINATOR = ',',
			   TABLOCK);

		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------'

		-- TABLE 2 SOURCE DATA CRM
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.crm_prd_info';
		TRUNCATE TABLE Bronze.crm_prd_info

		PRINT '>> Inserting Data into: Bronze.crm_prd_info';
		BULK INSERT Bronze.crm_prd_info
		FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( FIRSTROW = 2,
			   FIELDTERMINATOR = ',',
			   TABLOCK);
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------'

		-- TABLE 3 SOURCE DATA CRM
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.crm_sales_details';
		TRUNCATE TABLE Bronze.crm_sales_details

		PRINT '>> Inserting Data into: Bronze.crm_sales_details';
		BULK INSERT Bronze.crm_sales_details
		FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH ( FIRSTROW = 2,
			   FIELDTERMINATOR = ',',
			   TABLOCK);
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------'

		PRINT '-----------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------------------------------';

		-- TABLE 4 SOURCE DATA ERP
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.erp_cust_az12';
		TRUNCATE TABLE Bronze.erp_cust_az12

		PRINT '>> Inserting Data into: Bronze.erp_cust_az12';
		BULK INSERT Bronze.erp_cust_az12
		FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH ( FIRSTROW = 2,
			   FIELDTERMINATOR = ',',
			   TABLOCK);
		SET @End_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '------------------'

		-- TABLE 5 SOURCE DATA ERP
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.erp_loc_a101';
		TRUNCATE TABLE Bronze.erp_loc_a101

		PRINT '>> Inserting Data into: Bronze.erp_loc_a101';
		BULK INSERT Bronze.erp_loc_a101
		FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH ( FIRSTROW = 2,
			   FIELDTERMINATOR = ',',
			   TABLOCK);
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------'

		-- TABLE 6 SOURCE DATA ERP
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data into: Bronze.erp_px_cat_g1v2';
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH ( FIRSTROW = 2,
			   FIELDTERMINATOR = ',',
			   TABLOCK);
		SET @End_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '-------------------'

		SET @Batch_end_time = GETDATE();
		PRINT '========================================='
		PRINT 'LOADING OF BRONZE LAYER IS COMPLETED';
		PRINT '>>> Total Load Duration: ' + CAST(DATEDIFF(Second, @Batch_start_time, @Batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '========================================='
	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + Error_Message();
		PRINT 'ERROR NUMBER' + CAST(Error_Number() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST(Error_State() AS NVARCHAR);
		PRINT '================================================='
	END CATCH
END

-- ===============================================
-- Execute the Stored Procedure created above
-- ===============================================
EXEC Bronze.load_procedure
