/*
===============================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================
Script Purpose:
	This stored procedure performs the ETL (Extract, Transform, Load) process to 
	populate the 'silver' schema tables from the bronze schema.
	It performs:
	- Truncates the silver tables.
	- Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
	NONE
	This stored procedure does not accept any parameters or return any values.
Usage Example:
	CALL silver.load_silver();
To view the contents of the procedure:
SELECT pg_get_functiondef('silver.load_bronze'::regproc);
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
    LANGUAGE plpgsql
    AS $procedure$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time timestamp;
    batch_end_time timestamp;
BEGIN
    batch_start_time = current_timestamp;
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Loading the Silver Layer';
    RAISE NOTICE '=====================================';

    RAISE NOTICE '-------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '-------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting data into: crm_cust_info';
    INSERT INTO silver.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    )
    select
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname) as cst_lastname,
        CASE when UPPER(TRIM(cst_marital_status))='S' then 'Single'
            when UPPER(TRIM(cst_marital_status))='M' then 'Married'
            else 'n/a'
        END cst_marital_status, -- Normalize marital status to readable format
        CASE when UPPER(TRIM(cst_gndr))='F' then 'Female'
            when UPPER(TRIM(cst_gndr))='M' then 'Male'
            else 'n/a'
        END cst_gndr, -- Normalize gender values to readable format
        cst_create_date

    from (select *,
              row_number() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
          from bronze.crm_cust_info) t
    where flag_last=1; -- Select the most recent record per customer
        end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting data into: crm_prd_info';
    INSERT into silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    select
        prd_id,
        REPLACE(substr(prd_key, 1, 5),'-','_') as cat_id, -- Extract category ID
        substr(prd_key,7,length(prd_key)) as prd_key, -- Extract product key
        prd_nm,
        COALESCE(prd_cost,0) as prd_cost,
        case upper(trim(prd_line))
            when 'M' then 'Mountain'
            when 'R' then 'Road'
            when 'S' then 'Other Sales'
            when 'T' then 'Touring'
            else 'n/a'
        end as prd_line, -- Map product line codes to descriptive values
        CAST(prd_start_dt as date) as prd_start_dt,
        CAST(LEAD(prd_start_dt) over (PARTITION BY prd_key order by prd_start_dt asc)-1 as date) as prd_end_dt -- Calculate end date one day before the next start date
    from bronze.crm_prd_info;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting data into: crm_sales_details';
    INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    select sls_ord_num,
           sls_prd_key,
           sls_cust_id,
           CASE WHEN sls_order_dt = 0 or length(sls_order_dt::TEXT) !=8 then NULL
            else (sls_order_dt::TEXT)::DATE
           END as sls_order_dt,
        CASE WHEN sls_ship_dt = 0 or length(sls_ship_dt::TEXT) !=8 then NULL
            else (sls_ship_dt::TEXT)::DATE
           END as sls_ship_dt,
        CASE WHEN sls_due_dt = 0 or length(sls_due_dt::TEXT) !=8 then NULL
            else (sls_due_dt::TEXT)::DATE
           END as sls_due_dt,
           CASE WHEN sls_sales IS NULL or sls_sales <=0 OR sls_sales != sls_quantity * abs(sls_price)
        then sls_quantity* abs(sls_price)
        else sls_sales
        end as sls_sales, -- Recalculate sales if original value is missing or incorrect
           sls_quantity,
           CASE WHEN sls_price is null or sls_price<=0
        then sls_sales/coalesce(sls_quantity,0)
        else sls_price -- Derive price if original value is invalid
        end as sls_price
    from bronze.crm_sales_details;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    RAISE NOTICE '-------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '-------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting data into: erp_cust_az12';
    INSERT into silver.erp_cust_az12(cid, bdate, gen)
    select
           CASE WHEN cid like 'NAS%' THEN substr(cid,4,length(cid)) -- Remove 'NAS' prefix if present
            else cid
            end as cid,
        CASE WHEN bdate>current_date THEN NULL
            else bdate
                end as bdate, -- Set future birthdates to NULL
        CASE
        when upper(trim(gen)) in ('MALE','M') then 'Male'
        when upper(trim(gen)) in ('F','FEMALE') then 'Female'
        else 'n/a'
        end as gen -- Normalize gender values and handle unknown cases
    from bronze.erp_cust_az12;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting data into: erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    select
        replace(cid, '-','') as cid,
        case when upper(trim(cntry)) = 'DE' then 'Germany'
            when upper(trim(cntry)) in ('US','USA','UNITED STATES') then 'United States'
                when upper(trim(cntry)) ='' or cntry is null then 'n/a'
                    else trim(cntry)
                        end as cntry -- Normalize and handle missing or blank country codes
    from bronze.erp_loc_a101;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    select id,
           cat,
           subcat,
           maintenance
    from bronze.erp_px_cat_g1v2;
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    batch_end_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '======================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE 'Total Duration: %s', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))::INTEGER;
    RAISE NOTICE '======================================';



EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'Table does not exist: %', SQLERRM;
        RAISE;
    WHEN undefined_function THEN
        RAISE NOTICE 'Function pg_read_csv does not exist or is not accessible: %', SQLERRM;
        RAISE;
    WHEN undefined_column THEN
        RAISE NOTICE 'Column issue: %', SQLERRM;
        RAISE;
    WHEN OTHERS THEN
        RAISE NOTICE 'Error details: %, %', SQLSTATE, SQLERRM;
        RAISE;
END;
$procedure$;