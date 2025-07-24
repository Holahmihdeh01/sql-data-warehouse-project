-- =======================================================================================
-- Procedure Name: bronze.load_table
-- Purpose:
--     This procedure automates the initial ETL process for the Bronze layer of a data warehouse.
--     It loads raw CRM and ERP CSV datasets into staging tables within the bronze schema.
--     Each table is truncated before loading to ensure fresh data is inserted during each run.
--     The procedure also tracks the load time for performance monitoring and debugging.
-- =======================================================================================

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_table AS
BEGIN
    DECLARE 
        @cust_info_start_time DATETIME, @cust_info_end_time DATETIME,
        @prd_info_start_time DATETIME, @prd_info_end_time DATETIME,
        @sales_details_start_time DATETIME, @sales_details_end_time DATETIME,
        @cust_AZ12_start_time DATETIME, @cust_AZ12_end_time DATETIME,
        @loc_a101_start_time DATETIME, @loc_a101_end_time DATETIME,
        @px_cat_g1v2_start_time DATETIME, @px_cat_g1v2_end_time DATETIME,
        @start_load_time DATETIME, @end_load_time DATETIME;

    BEGIN TRY
        SET @start_load_time = GETDATE();
        PRINT '===================================================';
        PRINT 'LOADING BRONZE CRM & ERP TABLES';
        PRINT '===================================================';

        -- Load CRM: Customer Info
        PRINT '>> TRUNCATING & LOADING: bronze.crm_cust_info';
        IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
            TRUNCATE TABLE bronze.crm_cust_info;
        SET @cust_info_start_time = GETDATE();
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\admin\Videos\SQL\Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @cust_info_end_time = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @cust_info_start_time, @cust_info_end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load CRM: Product Info
        PRINT '>> TRUNCATING & LOADING: bronze.crm_prd_info';
        IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
            TRUNCATE TABLE bronze.crm_prd_info;
        SET @prd_info_start_time = GETDATE();
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\admin\Videos\SQL\Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @prd_info_end_time = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @prd_info_start_time, @prd_info_end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load CRM: Sales Details
        PRINT '>> TRUNCATING & LOADING: bronze.crm_sales_details';
        IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
            TRUNCATE TABLE bronze.crm_sales_details;
        SET @sales_details_start_time = GETDATE();
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\admin\Videos\SQL\Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @sales_details_end_time = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @sales_details_start_time, @sales_details_end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load ERP: Customer AZ12
        PRINT '>> TRUNCATING & LOADING: bronze.erp_cust_az12';
        IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
            TRUNCATE TABLE bronze.erp_cust_az12;
        SET @cust_AZ12_start_time = GETDATE();
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\admin\Videos\SQL\Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @cust_AZ12_end_time = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @cust_AZ12_start_time, @cust_AZ12_end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load ERP: Location A101
        PRINT '>> TRUNCATING & LOADING: bronze.erp_loc_a101';
        IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
            TRUNCATE TABLE bronze.erp_loc_a101;
        SET @loc_a101_start_time = GETDATE();
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\admin\Videos\SQL\Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @loc_a101_end_time = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @loc_a101_start_time, @loc_a101_end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

        -- Load ERP: Product Category G1V2
        PRINT '>> TRUNCATING & LOADING: bronze.erp_px_cat_g1v2';
        IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        SET @px_cat_g1v2_start_time = GETDATE();
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\admin\Videos\SQL\Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @px_cat_g1v2_end_time = GETDATE();
        PRINT 'LOADING DURATION: ' + CAST(DATEDIFF(SECOND, @px_cat_g1v2_start_time, @px_cat_g1v2_end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '---------------';

    END TRY
    BEGIN CATCH
        PRINT '===================================================';
        PRINT '>> ERROR OCCURRED WHILE LOADING BRONZE LAYER';
        PRINT '>> ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT '>> ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '===================================================';
    END CATCH;

    SET @end_load_time = GETDATE();
    PRINT ' ';
    PRINT '===================================================';
    PRINT 'TOTAL LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_load_time, @end_load_time) AS NVARCHAR) + ' SECONDS';
    PRINT '===================================================';
END

-- Run the procedure:
-- EXEC bronze.load_table;
