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