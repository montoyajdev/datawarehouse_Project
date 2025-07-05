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
