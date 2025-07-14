-- Create tables from CRM source
-- Every column is a 1:1 replica from the source

-- CRM Source

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
  cst_id int
  , cst_key varchar
  , cst_firstname varchar
  , cst_lastname varchar
  , cst_marital_status varchar
  , cst_gndr varchar
  , cst_create_date date
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
  prd_id int
  , prd_key varchar
  , prd_nm varchar
  , prd_cost int
  , prd_line varchar
  , prd_start_dt date
  , prd_end_dt date
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
  sls_ord_num varchar
  , sls_prd_key varchar
  , sls_cust_id int
  , sls_order_dt int
  , sls_ship_dt int
  , sls_due_dt int
  , sls_sales int
  , sls_quantity int
  , sls_price int
);

-- ERP Source

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
  cid varchar
  , bdate date
  , gen varchar
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
  cid varchar
  , cntry varchar
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
  id varchar
  , cat varchar
  , subcat varchar
  , maintenance varchar
);
