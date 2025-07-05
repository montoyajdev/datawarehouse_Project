
 /*
==============================================
DDL Script: Create Silver Tables
==============================================
Script Purpose:
	This script creates tables in the 'silver' schema,
	dropping existing tables if they already exist.
	Run this script to re-define the DDL structure of 'silver' tables.
==============================================
*/

CREATE SCHEMA silver;

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
    cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date DATE,
	dwh_create_date date default current_date
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
    sls_ord_num varchar(50),
    sls_prd_key varchar (50),
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
	dwh_create_date date default current_date
);
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
    prd_id	int,
	cat_id varchar(50),
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost decimal(10,2),
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date,
	dwh_create_date date default current_date
);

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
    CID varchar(50),
    BDATE date,
    GEN varchar(50),
	dwh_create_date date default current_date
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
    CID varchar(50),
    CNTRY varchar(50),
	dwh_create_date date default current_date
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
    ID varchar(50),
    CAT varchar(50),
    SUBCAT varchar(50),
    MAINTENANCE varchar(50),
	dwh_create_date date default current_date
);