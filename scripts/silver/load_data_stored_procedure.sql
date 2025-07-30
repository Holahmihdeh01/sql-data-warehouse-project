-- =======================================================================================
-- Procedure Name: silver.load_table
-- Purpose:
--     This procedure automates the ETL process for the Silver layer of the data warehouse.
--     It standardizes, deduplicates, and transforms CRM and ERP records from the Bronze layer.
--     Each Silver table is truncated before insert to ensure fresh, clean data is maintained.
--     Load durations are printed for performance monitoring.
-- =======================================================================================

DROP PROCEDURE load_silver_table
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_table AS
BEGIN
    DECLARE 
        @start_silver_load DATETIME, @end_silver_load DATETIME,
        @start_crm_cust_info DATETIME, @end_crm_cust_info DATETIME,
        @start_crm_prd_info DATETIME, @end_crm_prd_info DATETIME,
        @start_crm_sales_details DATETIME, @end_crm_sales_details DATETIME,
        @start_erp_cust_az12 DATETIME, @end_erp_cust_az12 DATETIME,
        @start_erp_loc_a101 DATETIME, @end_erp_loc_a101 DATETIME,
        @start_erp_px_cat_g1v2 DATETIME, @end_erp_px_cat_g1v2 DATETIME;

    BEGIN TRY
        SET @start_silver_load = GETDATE();
        PRINT '===================================================';
        PRINT '>> LOADING SILVER CRM & ERP TABLES';
        PRINT '===================================================';

        -- Load CRM: Customer Info
        PRINT '>> TRUNCATING & LOADING: silver.crm_cust_info';
        IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
            TRUNCATE TABLE silver.crm_cust_info;
        SET @start_crm_cust_info = GETDATE();
        INSERT INTO silver.crm_cust_info 
            (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
                WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END,
            CASE 
                WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
                WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS RecentRecord
            FROM bronze.crm_cust_info
        ) AS ranked
        WHERE RecentRecord = 1 AND cst_id IS NOT NULL;
        SET @end_crm_cust_info = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @start_crm_cust_info, @end_crm_cust_info) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load CRM: Product Info
        PRINT '>> TRUNCATING & LOADING: silver.crm_prd_info';
        IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
            TRUNCATE TABLE silver.crm_prd_info;
        SET @start_crm_prd_info = GETDATE();
        INSERT INTO silver.crm_prd_info
            (prd_id, prd_key, prd_derived_cat, prd_derived_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        SELECT
            prd_id,
            prd_key,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            COALESCE(prd_cost, 0),
            CASE 
                WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
                WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
                WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Standard'
                WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
                ELSE 'n/a'
            END,
            prd_start_dt,
            prd_derived_end_dt
        FROM (
            SELECT *,
                   RANK() OVER(PARTITION BY prd_key ORDER BY prd_end_dt DESC) AS recent_product_record,
                   DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_derived_end_dt
            FROM bronze.crm_prd_info
        ) AS ranked
        WHERE recent_product_record = 1;
        SET @end_crm_prd_info = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @start_crm_prd_info, @end_crm_prd_info) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load CRM: Sales Details
        PRINT '>> TRUNCATING & LOADING: silver.crm_sales_details';
        IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
            TRUNCATE TABLE silver.crm_sales_details;
        SET @start_crm_sales_details = GETDATE();
        INSERT INTO silver.crm_sales_details
            (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN LEN(sls_order_dt) <> 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN LEN(sls_ship_dt) <> 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN LEN(sls_due_dt) <> 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR (sls_quantity * sls_price) != sls_sales THEN ABS(sls_price * sls_quantity)
                ELSE sls_sales
            END,
            CASE 
                WHEN sls_quantity IS NULL OR sls_quantity <= 0 THEN ABS(sls_sales / sls_price)
                ELSE sls_quantity
            END,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;
        SET @end_crm_sales_details = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @start_crm_sales_details, @end_crm_sales_details) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load ERP: Customer AZ12
        PRINT '>> TRUNCATING & LOADING: silver.erp_cust_az12';
        IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
            TRUNCATE TABLE silver.erp_cust_az12;
        SET @start_erp_cust_az12 = GETDATE();
        INSERT INTO silver.erp_cust_az12
            (c_id, bdate, gen)
        SELECT 
            CASE WHEN LEN(c_id) = 13 THEN SUBSTRING(c_id, 4, LEN(c_id)) ELSE c_id END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;
        SET @end_erp_cust_az12 = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @start_erp_cust_az12, @end_erp_cust_az12) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load ERP: Location A101
        PRINT '>> TRUNCATING & LOADING: silver.erp_loc_a101';
        IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
            TRUNCATE TABLE silver.erp_loc_a101;
        SET @start_erp_loc_a101 = GETDATE();
        INSERT INTO silver.erp_loc_a101
            (cid, cntry)
        SELECT 
            CASE WHEN cid LIKE '___-%' AND LEN(cid) = 11 THEN REPLACE(cid, '-', '') ELSE cid END,
            CASE
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
                WHEN TRIM(UPPER(cntry)) IN ('US', 'USA') THEN 'United States'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;
        SET @end_erp_loc_a101 = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @start_erp_loc_a101, @end_erp_loc_a101) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load ERP: Product Category G1V2
        PRINT '>> TRUNCATING & LOADING: silver.erp_px_cat_g1v2';
        IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
        SET @start_erp_px_cat_g1v2 = GETDATE();
        INSERT INTO silver.erp_px_cat_g1v2
            (id, cat, subcat, maintenance)
        SELECT * FROM bronze.erp_px_cat_g1v2;
        SET @end_erp_px_cat_g1v2 = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @start_erp_px_cat_g1v2, @end_erp_px_cat_g1v2) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        SET @end_silver_load = GETDATE();
        PRINT '===================================================';
        PRINT 'TOTAL LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_silver_load, @end_silver_load) AS NVARCHAR) + ' SECONDS';
        PRINT '===================================================';
    END TRY
    BEGIN CATCH
        PRINT '===================================================';
        PRINT '>> ERROR OCCURRED WHILE LOADING SILVER LAYER';
        PRINT '>> ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT '>> ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '===================================================';
    END CATCH
END;

-- Run the procedure:
-- EXEC silver.load_table;
