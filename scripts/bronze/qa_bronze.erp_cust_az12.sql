-- remove the first three letters
Select cid,
       CASE WHEN cid like 'NAS%' THEN substr(cid,4,length(cid))
    else cid
        end as cid
from bronze.erp_cust_az12;

-- Identify out of range dates
select DISTINCT
    bdate from bronze.erp_cust_az12
where bdate<'1924-01-01' or bdate>CURRENT_DATE;

-- Data standardization and consistency
-- view the different genders
select distinct gen from bronze.erp_cust_az12;
-- make values consistent
select gen,
        CASE
    when upper(trim(gen)) in ('MALE','M') then 'Male'
    when upper(trim(gen)) in ('F','FEMALE') then 'Female'
    else 'n/a'
    end as gen
from bronze.erp_cust_az12;