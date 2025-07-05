-- check for invalid dates
select * from bronze.crm_sales_details
where sls_order_dt>crm_sales_details.sls_ship_dt or sls_order_dt>crm_sales_details.sls_due_dt;

-- Sales must be quantity * price
-- values must not be null, zero, or negative

select DISTINCT
    sls_sales as old,
    sls_quantity,
    sls_price,
    CASE WHEN sls_sales IS NULL or sls_sales <=0 OR sls_sales != sls_quantity * abs(sls_price)
    then sls_quantity* abs(sls_price)
    else sls_sales
    end as sls_sales,
    CASE WHEN sls_price is null or sls_price<=0
    then sls_sales/coalesce(sls_quantity,0)
    else sls_price
    end as sls_price
from bronze.crm_sales_details
where sls_sales != (sls_quantity*sls_price)
or sls_sales<=0 or sls_sales is null;