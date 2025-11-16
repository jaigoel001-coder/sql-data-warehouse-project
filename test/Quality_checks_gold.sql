/*
================================================================================
Quality Checks
================================================================================
Script Purpose:
  This script performs quality checks to validate the integrity, consistency,
  and accuracy of gold layer. These checks ensure:
  - Uniqueness of surrogate keys in dimension tables.
  - Referential integrity between facts and dimension tables.
  - Validation of relationships in the data model for analytical purposes.

Usage Notes:
  - Run these checks after data loading silver layer.
  - Investigate and resolve any discrepancies found during the checks.
================================================================================
*/

-- =============================================================================
-- Checking 'gold.dim_customers'
-- =============================================================================
-- Check the data Integration

SELECT DISTINCT
	 ci.cst_gndr,
	 ca.Gen,
	 CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the Master for gender info
		  ELSE COALESCE(ca.Gen, 'n/a')
	 END AS new_gen
FROM SILVER.crm_cust_info AS ci
LEFT JOIN Silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.CID
LEFT JOIN Silver.erp_loc_a101 AS la
ON ci.cst_key = la.CID

-- ============================================================================
-- Checking 'gold.product_key'
-- ============================================================================

SELECT prd_key, COUNT(*) FROM (
SELECT
	 pn.prd_id,
	 pn.cat_id,
	 pn.prd_key,
	 pn.prd_nm,
	 pn.prd_cost,
	 pn.prd_line,
	 pn.prd_start_dt,
	 pc.CAT,
	 pc.SUBCAT,
	 pc.MAINTENANCE
FROM Silver.crm_prd_info AS pn
LEFT JOIN Silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.ID
WHERE prd_end_dt IS NULL   -- Filter out all historical data
)t
GROUP BY prd_key
HAVING COUNT(*) > 1

-- ===========================================================================
-- Checking 'gold.fact_sales'
-- ===========================================================================
-- Check the data model connectivity between fact and dimensions

SELECT *
FROM Gold.fact_sales fs
LEFT JOIN Gold.dim_customers c
ON fs.Customer_key = c.Customer_key
LEFT JOIN Gold.dim_products p
ON fs.product_key = p.product_key
WHERE c.Customer_key IS NULL OR
	    p.product_key IS NULL


ORDER BY 1, 2
