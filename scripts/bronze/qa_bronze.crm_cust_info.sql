-- View duplciate cst_id
Select cst_id, count(cst_id)
from bronze.crm_cust_info
group by cst_id
having count(cst_id)>1 or cst_id is null;

-- Create a flag by cst_id for each row to notify latest create date
select *,
row_number() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
from bronze.crm_cust_info;

-- view which rows have a flag greater than 1
Select * from (select *,
row_number() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
from bronze.crm_cust_info) t
where flag_last>1;

-- View unique values for cst_gndr
select DISTINCT cst_gndr
from bronze.crm_cust_info;

-- View which values have a leading or trailing space
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);