-- ============================================================================================
-- Script Purpose:
-- This script creates views for the GOLD layer in the Data Warehouse. 
-- These views join cleaned and transformed SILVER layer tables to create analytical models:
--   - gold.dim_customers
--   - gold.dim_product
--   - gold.fact_sales
-- ============================================================================================

USE DataWarehouse;
GO

-- Drop and create Customer Dimension View
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER(ORDER BY c.cst_id) AS customer_key,
    c.cst_id AS customer_id,
    c.cst_key AS customer_number,
    c.cst_firstname AS first_name,
    c.cst_lastname AS last_name,
    CASE 
        WHEN c.cst_gndr != 'n/a' THEN c.cst_gndr
        ELSE COALESCE(e.gen, 'n/a')
    END AS gender,
    c.cst_marital_status AS marital_status,
    l.cntry AS country,
    e.bdate AS birth_date,
    c.cst_create_date AS create_date
FROM silver.crm_cust_info AS c
LEFT JOIN silver.erp_cust_az12 AS e
    ON c.cst_key = e.c_id
LEFT JOIN silver.erp_loc_a101 AS l
    ON c.cst_key = l.cid;
GO

-- Drop and create Product Dimension View
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS 
SELECT 
    ROW_NUMBER() OVER(ORDER BY prd.prd_start_dt, prd.prd_derived_key) AS product_key,
    prd.prd_id AS product_id,
    prd.prd_derived_key AS product_number,
    prd.prd_nm AS product_name,
    prd.prd_derived_cat AS category_id,
    px.cat AS category,
    px.subcat AS subcategory,
    px.maintenance AS maintenance,
    prd.prd_cost AS cost,
    prd.prd_start_dt AS product_start_date,
    prd.prd_end_dt AS product_end_date
FROM silver.crm_prd_info AS prd
LEFT JOIN silver.erp_px_cat_g1v2 AS px
    ON prd.prd_derived_cat = px.id;
GO

-- Drop and create Sales Fact View
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    c.sls_ord_num AS order_id,
    pr.product_key AS product_key, 
    cu.customer_key AS customer_key,
    c.sls_order_dt AS order_date,
    c.sls_ship_dt AS ship_date,
    c.sls_due_dt AS sales_due_date,
    c.sls_price AS price,
    c.sls_quantity AS quantity,
    c.sls_sales AS sales
FROM silver.crm_sales_details AS c
LEFT JOIN gold.dim_customers AS cu
    ON c.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_product AS pr
    ON c.sls_prd_key = pr.product_number;
GO
