-- Imports data from CSV to table

TRUNCATE bronze.crm_cust_info RESTART IDENTITY;
COPY bronze.crm_cust_info
FROM '/tmp/Datasets/source_crm/cust_info.csv'
WITH (
  DELIMITER ','
  , HEADER TRUE
  , NULL ''
);

TRUNCATE bronze.crm_prd_info RESTART IDENTITY;
COPY bronze.crm_prd_info
FROM '/tmp/Datasets/source_crm/prd_info.csv'
WITH (
  DELIMITER ','
  , HEADER TRUE
  , NULL ''
);

TRUNCATE bronze.crm_sales_details RESTART IDENTITY;
COPY bronze.crm_sales_details
FROM '/tmp/Datasets/source_crm/sales_details.csv'
WITH (
  DELIMITER ','
  , HEADER TRUE
  , NULL ''
);