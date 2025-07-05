CREATE VIEW gold.dim_customers AS
select
    row_number() over (order by cst_id) as customer_key,
    ci.cst_id AS customer_id,
       ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
       ci.cst_lastname as last_name,
       la.cntry as country,
       ci.cst_marital_status as marital_status,
       CASE WHEN ci.cst_gndr !='n/a' then ci.cst_gndr -- CRM is the master for gender info
        else COALESCE(ca.gen,'n/a')
        end as gender,
    ca.bdate as birthdate,
       ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid;