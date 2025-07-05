CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
    RAISE NOTICE 'Loading the Bronze Layer';
    RAISE NOTICE '=====================================';

    RAISE NOTICE '-------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '-------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> Inserting data into: crm_cust_info';
    COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_crm\cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> Inserting data into: crm_prd_info';
    COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_crm\prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> Inserting data into: crm_sales_details';
    COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_crm\sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    RAISE NOTICE '-------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '-------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>> Inserting data into: erp_cust_az12';
    COPY bronze.erp_cust_az12 (cid, bdate, gen)
    FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_erp\CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>> Inserting data into: erp_loc_a101';
    COPY bronze.erp_loc_a101 (cid, cntry)
    FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_erp\LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    start_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting data into: erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    FROM 'C:\Program Files\PostgreSQL\17\data\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '------------------------------------';

    batch_end_time = CURRENT_TIMESTAMP;
    RAISE NOTICE '======================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
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