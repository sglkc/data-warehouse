-- Create tables from ERP source
-- Every column data type is a 1:1 replica from the source

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
