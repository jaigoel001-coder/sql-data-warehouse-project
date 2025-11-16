/*
================================================================
DDL Script: Create Gold View
================================================================
Script Purpose:
This script create views for the gold layer in the data warehouse.
The gold layer represent the final dimension and fact tables (star schema)

Each view perform transformation and combine data from the silver layer
to produce a clean enriched and business ready dataset.

Usage:
  - These views can be queried directly for analytics and reporting.
=================================================================
*/

-- =================================================================
-- Create Dimension: gold.dim_customers
-- =================================================================

IF Object_ID ('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW Gold.dim_customers AS   --  Create a View for the Gold Layer
SELECT
	 ROW_NUMBER() OVER (ORDER BY cst_id) AS Customer_key,  -- Creating Surrogate Key
	 ci.cst_id AS Customer_id,
	 ci.cst_key AS Customer_Number,
	 ci.cst_firstname AS First_name,
	 ci.cst_lastname AS Last_name,
	 ci.cst_marital_status AS Marital_status,
	 CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the Master for gender info
		  ELSE COALESCE(ca.Gen, 'n/a')
	 END AS Gender,
	 la.CNTRY AS Country,
	 ca.BDate AS Birth_date,
	 ci.cst_create_date AS Create_date
FROM SILVER.crm_cust_info AS ci
LEFT JOIN Silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.CID
LEFT JOIN Silver.erp_loc_a101 AS la
ON ci.cst_key = la.CID

-- ============================================================
-- Create Dimension: Gold.dim_products
-- ============================================================

  IF Object_ID ('gold.dim_products', 'V') IS NOT NULL
     DROP VIEW gold.dim_products;
GO

CREATE VIEW Gold.dim_products AS
SELECT
	 ROW_NUMBER() OVER (ORDER BY pn.prd_key, pn.prd_start_dt) AS product_key,   -- Create a Surrogate Key
	 pn.prd_id AS product_id,
	 pn.prd_key AS product_number,
	 pn.prd_nm AS product_name,
	 pn.cat_id AS category_id,
	 pc.CAT AS category,
	 pc.SUBCAT AS subcategry,
	 pc.MAINTENANCE,
	 pn.prd_cost AS cost,
	 pn.prd_line AS product_line,
	 pn.prd_start_dt AS start_date
FROM Silver.crm_prd_info AS pn
LEFT JOIN Silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.ID
WHERE prd_end_dt IS NULL   -- Filter out all historical data

-- ===========================================================
-- Create Dimension: Gold.fact_sales
-- ===========================================================

  IF Object_ID ('gold.fact_sales', 'V') IS NOT NULL
     DROP VIEW gold.fact_sales;
GO

CREATE VIEW Gold.fact_sales AS
SELECT
	 sd.sls_ord_num AS Order_number,
	 pr.product_key,
	 cu.Customer_key,
	 sd.sls_order_dt AS Order_date, 
	 sd.sls_ship_dt AS Shipping_date,
	 sd.sls_due_dt AS Due_date,
	 sd.sls_sales AS Sales_amount,
	 sd.sls_quatity AS Quantity,
	 sd.sls_price AS Price
FROM Silver.crm_sales_details AS sd
LEFT JOIN Gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN Gold.dim_customers AS cu
ON sd.sls_cust_id = cu.Customer_id

