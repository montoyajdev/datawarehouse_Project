CREATE DATABASE Datawarehouse;

CREATE SCHEMA bronze;

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
    cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_material_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num varchar(50),
    sls_prd_key varchar (50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
    prd_id	int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost decimal(10,2),
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    CID varchar(50),
    BDATE date,
    GEN varchar(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    CID varchar(50),
    CNTRY varchar(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
    ID varchar(50),
    CAT varchar(50),
    SUBCAT varchar(50),
    MAINTENANCE varchar(50)
);