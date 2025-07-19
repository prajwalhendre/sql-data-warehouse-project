/*
=====================================
stored procedure: load bronze layer (from source to bronze)
=====================================
usage example:
  exec bronze.load_bronze;
=====================================
*/

create or alter procedure bronze.load_bronze AS
begin 
    declare @total_start_time datetime, @total_end_time datetime, @start_time datetime, @end_time datetime;
    begin TRY
        set @total_start_time = getdate();
        print '========================';
        print 'loading the bronze layer';
        print '========================';
        print 'loading crm table';
        print '========================';
        set @start_time = getdate();
        print '>> truncating table: bronze.crm_cust_info'
        truncate table bronze.crm_cust_info;
        print '>> importing table: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a', 
            TABLOCK
        );
        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> ------------';
        set @start_time = getdate();
        print '>> truncating table: bronze.crm_prd_info'
        truncate table bronze.crm_prd_info;
        print '>> importing table: bronze.crm_prd_info'
        bulk insert bronze.crm_prd_info
        from "/var/opt/mssql/data/prd_info.csv"
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> ------------';
        set @start_time = getdate();
        print '>> truncating table: bronze.crm_sales_details'
        truncate table bronze.crm_sales_details;
        print '>> importing table: bronze.crm_sales_details'

        bulk insert bronze.crm_sales_details
        from "/var/opt/mssql/data/sales_details.csv"
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        print '========================';
        print 'loading erp table';
        print '========================';
        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> ------------';
        set @start_time = getdate();
        print '>> truncating table: bronze.erp_cust_az12'
        truncate table bronze.erp_cust_az12;
        print '>> importing table: bronze.erp_cust_az12'

        bulk insert bronze.erp_cust_az12
        from "/var/opt/mssql/data/CUST_AZ12.csv"
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> ------------';
        set @start_time = getdate();
        print '>> truncating table: bronze.erp_loc_a101'
        truncate table bronze.erp_loc_a101;
        print '>> importing table: bronze.erp_loc_a101'

        bulk insert bronze.erp_loc_a101
        from "/var/opt/mssql/data/LOC_A101.csv"
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> ------------';
        set @start_time = getdate();
        print '>> truncating table: bronze.erp_px_cat_g1v2'
        truncate table bronze.erp_px_cat_g1v2;
        print '>> importing table: bronze.erp_px_cat_g1v2'    
        bulk insert bronze.erp_px_cat_g1v2
        from "/var/opt/mssql/data/PX_CAT_G1V2.csv"
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> ------------';
        set @total_end_time = getdate();
        print '>> TOTAL load duration: ' + cast(datediff(second, @total_start_time, @total_end_time) as nvarchar) + ' seconds';
    end TRY
    begin catch

        print '========================'
        print 'error occured during loading bronze layer'
        print 'error message' + error_message();
        print 'error message' + cast(error_number() as nvarchar);
        print '========================'
    end catch
end
