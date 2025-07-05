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

