/*
==========================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==========================================================================
Script Purpose:
  This stored procedure perform the ETL (Extract, Transform, Load) process
  to populate the 'Silver' schema tables from the 'Bronze' schema.
Actions Performed:
  - Truncate Silver Tables.
  - Insert transformed and cleaned data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC Silver.load_Silver;
============================================================================
*/

CREATE OR ALTER PROCEDURE Silver.load_Silver AS
BEGIN
	DECLARE @Start_time DATETIME, @End_time DATETIME, @Batch_start_time DATETIME, @Batch_end_time DATETIME;
	BEGIN TRY
		SET @Batch_start_time = GETDATE();
		PRINT '=====================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=====================================================';

		PRINT '----------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------------';

		-- Loading Silver.crm_cust_info
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_cust_info'
		TRUNCATE TABLE Silver.crm_cust_info
		PRINT '>> Inserting Data Into: Silver.crm_cust_info'
		INSERT INTO Silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		SELECT
			 cst_id,
			 cst_key,
			 TRIM(cst_firstname) AS cst_firstname,
			 TRIM(cst_lastname) AS cst_lastname,
			 CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				  WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				  ELSE 'N/A'
			 END cst_marital_status,
			 CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				  WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				  ELSE 'N/A'
			 END cst_gndr,
			 cst_create_date
		FROM (
				SELECT
					 *,
					 ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
				FROM Bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
			 )t
		WHERE Flag_last = 1;
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> -------------------';

		-- Loading Silver.crm_prd_info
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_prd_info'
		TRUNCATE TABLE Silver.crm_prd_info
		PRINT '>> Inserting Data Into: Silver.crm_prd_info'
		INSERT INTO Silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line,prd_start_dt, prd_end_dt)
		SELECT
			 prd_id,
			 REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS Cat_id, -- Extract category ID
			 SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract Product Key
			 prd_nm,
			 ISNULL(prd_cost, 0) AS prd_cost,
			 CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				  WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				  WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				  WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				  ELSE 'N/A'
			 END AS prd_line, -- Map product line codes to descriptive values
			 prd_start_dt,
			 DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt -- Calculate End date as one day before the next start date
		FROM Bronze.crm_prd_info;
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> -------------------';

		-- Loading Silver.crm_sales_details
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_sales_details'
		TRUNCATE TABLE Silver.crm_sales_details
		PRINT '>> Inserting Data Into: Silver.crm_sales_details'
		INSERT INTO Silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quatity, sls_price)
		SELECT
			 sls_ord_num,
			 sls_prd_key,
			 sls_cust_id,
			 CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				  ELSE CAST(CAST(sls_order_dt AS nvarchar) AS date)
			 END AS sls_order_dt,
			 CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				  ELSE CAST(CAST(sls_ship_dt AS nvarchar) AS date)
			 END AS sls_ship_dt,
			 CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				  ELSE CAST(CAST(sls_due_dt AS nvarchar) AS date)
			 END AS sls_due_dt,
			 CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quatity * ABS(sls_price)
					THEN sls_quatity * ABS(sls_price)
				 ELSE sls_sales
			 END AS sls_sales,
			 sls_quatity,
			 CASE WHEN sls_price IS NULL OR sls_price <=0
					THEN sls_sales / NULLIF(sls_quatity, 0)
				 ELSE sls_price
			 END AS sls_price
		FROM Bronze.crm_sales_details;
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------';

		PRINT '----------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '----------------------------------------';

		-- Loading Silver.erp_cust_az12
		SET @Start_time = GETDATE();
		PRINT '>> Truncating table: Silver.erp_cust_az12'
		TRUNCATE TABLE Silver.erp_cust_az12
		PRINT '>> Inserting Data Into: Silver.erp_cust_az12'
		INSERT INTO Silver.erp_cust_az12 (CID, BDate, Gen)
		SELECT
			 CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))  -- Remove 'NAS' Prefix if present
				  ELSE CID
			 END AS CID,
			 CASE WHEN BDate > GETDATE() THEN NULL
				  ELSE BDate
			 END AS Bdate, -- Set future birthdates to NULL
			 CASE WHEN UPPER(TRIM(Gen)) IN ('F', 'FEMALE') THEN 'Female'
				  WHEN UPPER(TRIM(Gen)) IN ('M', 'MALE') THEN 'Male'
				  ELSE 'N/A'
			 END Gen -- Normalize gender values and handle unknown cases
		FROM Bronze.erp_cust_az12;
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds'
		PRINT '>> --------------------';

		-- Loading Silver.erp_loc_a101
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_loc_a101'
		TRUNCATE TABLE Silver.erp_loc_a101
		PRINT '>> Inserting Data Into: Silver.erp_loc_a101'
		INSERT INTO Silver.erp_loc_a101 (CID, CNTRY)
		SELECT
			 REPLACE(CID, '-', '') AS CID,
			 CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				  WHEN TRIM(CNTRY) IN ('USA', 'US') THEN 'United States'
				  WHEN TRIM(CNTRY) IS NULL OR CNTRY = '' THEN 'N/A'
				  ELSE TRIM(CNTRY)
			 END AS CNTRY   --- Normalize and handle missing or blank country codes
		FROM Bronze.erp_loc_a101;
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------';

		-- Loading Silver.erp_px_cat_g1v2
		SET @Start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_px_cat_g1v2';
		TRUNCATE TABLE Silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: Silver.erp_px_cat_g1v2';
		INSERT INTO Silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
		SELECT
			 ID,
			 CAT,
			 SUBCAT,
			 MAINTENANCE
		FROM Bronze.erp_px_cat_g1v2
		SET @End_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(Second, @Start_time, @End_time) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------';

		SET @Batch_end_time = GETDATE();
		PRINT '==================================================================================================================';
		PRINT 'Loading of Silver Layer is Completed';
		PRINT '>>> Total Load Duration: ' + CAST(DATEDIFF(Second, @Batch_start_time, @Batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '==================================================================================================================';
	END TRY

	BEGIN CATCH
		PRINT '=================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE' + Error_Message();
		PRINT 'ERROR NUMBER' + CAST(Error_Number() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST(Error_State() AS NVARCHAR);
		PRINT '=================================================';
	END CATCH
END

-- ===========================================
-- Execute the Silver Layer Stored Procedure
-- ===========================================
EXEC Silver.load_Silver
