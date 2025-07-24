-- ============================================================================================
-- Script Purpose:
-- This script initializes the BRONZE layer of the Data Warehouse by creating raw staging tables 
-- for CRM and ERP source systems. These tables store untransformed data for further processing 
-- in the SILVER and GOLD layers.
-- ============================================================================================

USE DataWarehouse;
GO

-- Drop and create CRM Customer Info Table
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(25),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(1),
    cst_gndr NVARCHAR(1),
    cst_create_date DATE
);

-- Drop and create CRM Product Info Table
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(25),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(5),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- Drop and create CRM Sales Details Table
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(25),
    sls_prd_key NVARCHAR(25),
    sls_cust_id INT,
    sls_order_dt INT,     -- Format likely YYYYMMDD
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Drop and create ERP Customer Table (AZ12)
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    c_id NVARCHAR(25),
    bdate DATE,
    gen NVARCHAR(25)
);

-- Drop and create ERP Location Table (A101)
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(25),
    cntry NVARCHAR(25)
);

-- Drop and create ERP Product Category Table (PX_CAT_G1V2)
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(25),
    cat NVARCHAR(25),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(10)
);
