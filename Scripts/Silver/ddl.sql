-- Create tables from CRM source
-- Every column is a 1:1 replica from the source

-- CRM Source

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
  cst_id int
  , cst_key varchar
  , cst_firstname varchar
  , cst_lastname varchar
  , cst_marital_status varchar
  , cst_gndr varchar
  , cst_create_date date
  , dwh_create_date timestamp DEFAULT now()
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
  prd_id int
  , cat_id varchar
  , prd_key varchar
  , prd_nm varchar
  , prd_cost int
  , prd_line varchar
  , prd_start_dt date
  , prd_end_dt date
  , dwh_create_date timestamp DEFAULT now()
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
  sls_ord_num varchar
  , sls_prd_key varchar
  , sls_cust_id int
  , sls_order_dt date
  , sls_ship_dt date
  , sls_due_dt date
  , sls_sales int
  , sls_quantity int
  , sls_price int
  , dwh_create_date timestamp DEFAULT now()
);

-- ERP Source

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
  cid varchar
  , bdate date
  , gen varchar
  , dwh_create_date timestamp DEFAULT now()
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
  cid varchar
  , cntry varchar
  , dwh_create_date timestamp DEFAULT now()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
  id varchar
  , cat varchar
  , subcat varchar
  , maintenance varchar
  , dwh_create_date timestamp DEFAULT now()
);
