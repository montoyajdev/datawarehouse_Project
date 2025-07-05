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