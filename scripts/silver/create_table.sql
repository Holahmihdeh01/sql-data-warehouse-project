-- ============================================================================================
-- Script Purpose:
-- This script initializes the SILVER layer of the Data Warehouse by creating structured and 
-- cleaned tables for CRM and ERP source systems. These tables store deduplicated and standardized 
-- data from the BRONZE layer, ready for business reporting and further transformation in the GOLD layer.
-- ============================================================================================

USE DataWarehouse;
GO

-- Drop and create CRM Customer Info Table
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(25),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(25),
    cst_gndr NVARCHAR(25),
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop and create CRM Product Info Table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(25),
    prd_derived_cat NVARCHAR(25),
    prd_derived_key NVARCHAR(25),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(25),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop and create CRM Sales Details Table
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(25),
    sls_prd_key NVARCHAR(25),
    sls_cust_id INT,
    sls_order_dt DATE,     -- Converted from INT format (e.g., 20220101)
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop and create ERP Customer Table (AZ12)
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    c_id NVARCHAR(25),
    bdate DATE,
    gen NVARCHAR(25),
    dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop and create ERP Location Table (A101)
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(25),
    cntry NVARCHAR(25),
    dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Drop and create ERP Product Category Table (PX_CAT_G1V2)
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR(25),
    cat NVARCHAR(25),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(25),
    dwh_create_date DATETIME DEFAULT GETDATE()
);
