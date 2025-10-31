/*
========================================================================================
Quality Checks
========================================================================================
Script Purpose:
  This script perform various quality checks for data consistency, accuracy, and
  standardization across the 'Silver' schema. It include checks for:
  - NULL or duplicate primary key.
  - Unwanted spaces in string field.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.
========================================================================================
*/

-- In order to load the data in SILVER LAYER do the quality check of primary key
-- Expectation: No Result

SELECT
	 cst_id,
	 COUNT(*)
FROM Bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1


SELECT *
FROM (
		SELECT
			 *,
			 ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
		FROM Bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	 )t
WHERE Flag_last = 1

-- Check for unwanted spaces
-- Expectations: No Results

SELECT
	 cst_firstname
FROM Bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- DATA Standardization and Consistency

SELECT DISTINCT cst_gndr
FROM Bronze.crm_cust_info


-- CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- EXPECTATION: NO RESULT

SELECT
	 prd_id,
	 COUNT(*)
FROM Bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT DISTINCT prd_id FROM Bronze.crm_prd_info

-- check the unwanted spaces in prd_nm column
SELECT prd_nm
FROM Bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--  Check for Invalid Date orders
SELECT *
FROM Bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- Check for the prd_start_dt and prd_end_dt column
SELECT
	  prd_id,
	  prd_key,
	  prd_nm,
	  prd_start_dt,
	  prd_end_dt,
	  DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt_test
FROM Bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- Check for Invalid Dates
-- (Negative numbers or ZEROS can't be cast to a date)
SELECT 
	  NULLIF(sls_order_dt, 0) sls_order_dt
FROM Bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

SELECT 
	  NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM Bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

SELECT 
	  NULLIF(sls_due_dt, 0) sls_due_dt
FROM Bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

-- Check for Invalid date orders
SELECT
	 *
FROM Bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check data consistency: between sales, quantity and price
-- >> Sales = Quantity * Price
-- >> Values must not be Null, Negative or Zero.

SELECT
	sls_sales AS sls_old_sales,
	sls_quatity,
	sls_price AS sls_old_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quatity * ABS(sls_price)
			THEN sls_quatity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <=0
			THEN sls_sales / NULLIF(sls_quatity, 0)
		 ELSE sls_price
	END AS sls_price
FROM Bronze.crm_sales_details
WHERE sls_sales != sls_quatity * sls_price
OR sls_sales <= 0 OR sls_quatity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quatity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quatity

-- DO THE TRANSFORMATION OF ERP_CUST_AZ12 TABLE
SELECT
	 CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
		  ELSE CID
	 END AS CID,
	 CASE WHEN BDate > GETDATE() THEN NULL
		  ELSE BDate
	 END AS Bdate,
	 CASE WHEN UPPER(TRIM(Gen)) IN ('F', 'FEMALE') THEN 'Female'
		  WHEN UPPER(TRIM(Gen)) IN ('M', 'MALE') THEN 'Male'
		  ELSE 'N/A'
	 END Gen
FROM Bronze.erp_cust_az12
WHERE BDate < '1924-01-01' OR BDate > GETDATE()

/*WHERE CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
		  ELSE CID
	 END NOT IN (SELECT DISTINCT cst_key FROM Silver.crm_cust_info)
	 */
--WHERE CID LIKE '%AW00011000%'

-- Quality check for type of gender
SELECT DISTINCT
	 Gen,
	 CASE WHEN Gen IN ('F', 'FEMALE') THEN 'Female'
		  WHEN Gen IN ('M', 'MALE') THEN 'Male'
		  ELSE 'N/A'
	 END Gen
FROM Bronze.erp_cust_az12
GROUP BY Gen

-- ====================================================
-- Transform another Table in silver layer erp_loc_a101
-- ====================================================
SELECT
	 REPLACE(CID, '-', '') AS CID,
	 CNTRY,
	 CASE WHEN CNTRY = 'DE' THEN 'Germany'
		  WHEN CNTRY IN ('USA', 'US') THEN 'United States'
		  WHEN CNTRY IS NULL OR CNTRY = '' THEN 'N/A'
		  ELSE CNTRY
	 END AS CNTRY
FROM Bronze.erp_loc_a101

--WHERE REPLACE(CID, '-', '') NOT IN (SELECT cst_key FROM Silver.crm_cust_info)

SELECT cst_key FROM Silver.crm_cust_info

SELECT DISTINCT
	 CNTRY AS Old_cntry,
	 CASE WHEN CNTRY = 'DE' THEN 'Germany'
		  WHEN CNTRY IN ('USA', 'US') THEN 'United States'
		  WHEN CNTRY IS NULL OR CNTRY = '' THEN 'N/A'
		  ELSE CNTRY
	 END AS CNTRY
FROM Bronze.erp_loc_a101
ORDER BY CNTRY

-- =======================================================================================
-- Transform another table erp_px_cat_g1v2 from Bronze layer and Insert into Silver layer
-- =======================================================================================
SELECT
	 ID,
	 CAT,
	 SUBCAT,
	 MAINTENANCE
FROM Bronze.erp_px_cat_g1v2

SELECT *
FROM Silver.crm_prd_info

-- Check for Unwanted spaces
SELECT
	 *
FROM Bronze.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT)
OR SUBCAT != TRIM(SUBCAT)
OR MAINTENANCE != TRIM(MAINTENANCE)

SELECT DISTINCT CAT FROM Bronze.erp_px_cat_g1v2
SELECT DISTINCT SUBCAT FROM Bronze.erp_px_cat_g1v2
SELECT DISTINCT MAINTENANCE FROM Bronze.erp_px_cat_g1v2

