INSERT INTO silver.erp_loc_a101 (cid, cntry)
select
    replace(cid, '-','') as cid,
    case when upper(trim(cntry)) = 'DE' then 'Germany'
        when upper(trim(cntry)) in ('US','USA','UNITED STATES') then 'United States'
            when upper(trim(cntry)) ='' or cntry is null then 'n/a'
                else trim(cntry)
                    end as cntry -- Normalize and handle missing or blank country codes
from bronze.erp_loc_a101;