/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure creates the tables where the cleaned/standardized data will live

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as
begin
    print '> truncating table silver.crm_cust_info'
    truncate table silver.crm_cust_info
    print '> inserting data into: silver.crm_cust_info'
    -- script for cleaning crm_cust_info
    insert into silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    select 
    cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case when Upper(Trim(cst_marital_status)) = 'S' then 'Single'
        when Upper(trim(cst_marital_status)) = 'M' then 'Married'
        else 'n/a'
    end cst_marital_status,
    case when Upper(Trim(cst_gndr)) = 'F' then 'Female'
        when Upper(trim(cst_gndr)) = 'M' then 'Male'
        else 'n/a'
    end cst_gndr,
    cst_create_date
    From(
        select *,
        ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
        from bronze.crm_cust_info
    )t where flag_last = 1;


    print '> truncating table silver.crm_prd_info'
    truncate table silver.crm_prd_info
    print '> inserting data into: silver.crm_prd_info'
    -- script for cleaning crm_prd_info
    insert into silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    select 
    prd_id,
    replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
    substring(prd_key, 7, len(prd_key)) as prd_key,
    prd_nm,
    isnull(prd_cost, 0) as prd_cost,
    case when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'R' then 'Road'
        when upper(trim(prd_line)) = 'S' then 'other Sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        else 'n/a'
    end as prd_line,
    prd_start_dt,
    dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt
    from bronze.crm_prd_info


    print '> truncating table silver.crm_sales_details'
    truncate table silver.crm_sales_details
    print '> inserting data into: silver.crm_sales_details'
    -- script for cleaning crm_sales_details
    insert into silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
        else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,
    case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
        else cast(cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
    case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
        else cast(cast(sls_due_dt as varchar) as date)
    end as sls_due_dt,
    case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
        then sls_quantity * abs(sls_price)
        else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price <= 0
        then sls_sales/nullif(sls_quantity, 0)
        else sls_price
    end as sls_price
    from bronze.crm_sales_details


    print '> truncating table silver.erp_cust_az12'
    truncate table silver.erp_cust_az12
    print '> inserting data into: silver.erp_cust_az12'
    -- script for cleaning erp_cust_az12
    insert into silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    select 
    case when cid like 'NAS%' then substring(cid, 4, len(cid))
        else cid
    end as cid,
    case when bdate > getdate() then null
        else bdate
    end as bdate,
    CASE
            when UPPER(TRIM(REPLACE(gen, char(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
            when UPPER(TRIM(REPLACE(gen, char(13), ''))) IN ('M', 'MALE') THEN 'Male'
            else 'n/a'
    end as gen
    from bronze.erp_cust_az12


    print '> truncating table silver.erp_loc_a101'
    truncate table silver.erp_loc_a101
    print '> inserting data into: silver.erp_loc_a101'
    -- script for cleaning erp.loc_a101
    insert into silver.erp_loc_a101 (
        cid,
        cntry
    )
    select 
    replace(cid, '-', '') cid,
    case
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
        end as cntry
    from bronze.erp_loc_a101 


    print '> truncating table silver.erp_px_cat_g1v2'
    truncate table silver.erp_px_cat_g1v2
    print '> inserting data into: silver.erp_px_cat_g1v2'
    -- script for cleaning er.px_cat_g1v2
    insert into silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM
        bronze.erp_px_cat_g1v2
end


