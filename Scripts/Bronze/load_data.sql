-- Imports data from CSV to table
-- This function truncates tables before importing

CREATE OR REPLACE FUNCTION load_bronze()
RETURNS void
LANGUAGE plpgsql
AS $$
  DECLARE
    start_time TIMESTAMP;
    start_load_time TIMESTAMP;
  BEGIN
    start_load_time = NOW();
    RAISE NOTICE 'Loading source data to bronze layer...';

    start_time = NOW();
    RAISE NOTICE 'Importing bronze.crm_cust_info';
    TRUNCATE bronze.crm_cust_info RESTART IDENTITY;
    COPY bronze.crm_cust_info
    FROM '/tmp/Datasets/source_crm/cust_info.csv'
    WITH (
      DELIMITER ','
      , HEADER TRUE
      , NULL ''
    );
    RAISE NOTICE 'Imported bronze.crm_cust_info in % ms', EXTRACT(EPOCH FROM (NOW() - start_time));
    
    start_time = NOW();
    RAISE NOTICE 'Importing bronze.crm_prd_info';
    TRUNCATE bronze.crm_prd_info RESTART IDENTITY;
    COPY bronze.crm_prd_info
    FROM '/tmp/Datasets/source_crm/prd_info.csv'
    WITH (
      DELIMITER ','
      , HEADER TRUE
      , NULL ''
    );
    RAISE NOTICE 'Imported bronze.crm_prd_info in % ms', EXTRACT(EPOCH FROM (NOW() - start_time));
    
    start_time = NOW();
    RAISE NOTICE 'Importing bronze.crm_sales_details';
    TRUNCATE bronze.crm_sales_details RESTART IDENTITY;
    COPY bronze.crm_sales_details
    FROM '/tmp/Datasets/source_crm/sales_details.csv'
    WITH (
      DELIMITER ','
      , HEADER TRUE
      , NULL ''
    );
    RAISE NOTICE 'Imported bronze.crm_sales_details in % ms', EXTRACT(EPOCH FROM (NOW() - start_time));

    start_time = NOW();
    RAISE NOTICE 'Importing bronze.erp_cust_az12';
    TRUNCATE bronze.erp_cust_az12 RESTART IDENTITY;
    COPY bronze.erp_cust_az12
    FROM '/tmp/Datasets/source_erp/CUST_AZ12.csv'
    WITH (
      DELIMITER ','
      , HEADER TRUE
      , NULL ''
    );
    RAISE NOTICE 'Imported bronze.erp_cust_az12 in % ms', EXTRACT(EPOCH FROM (NOW() - start_time));
    
    start_time = NOW();
    RAISE NOTICE 'Importing bronze.erp_loc_a101';
    TRUNCATE bronze.erp_loc_a101 RESTART IDENTITY;
    COPY bronze.erp_loc_a101
    FROM '/tmp/Datasets/source_erp/LOC_A101.csv'
    WITH (
      DELIMITER ','
      , HEADER TRUE
      , NULL ''
    );
    RAISE NOTICE 'Imported bronze.erp_loc_a101 in % ms', EXTRACT(EPOCH FROM (NOW() - start_time));
    
    start_time = NOW();
    RAISE NOTICE 'Importing bronze.erp_px_cat_g1v2';
    TRUNCATE bronze.erp_px_cat_g1v2 RESTART IDENTITY;
    COPY bronze.erp_px_cat_g1v2
    FROM '/tmp/Datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
      DELIMITER ','
      , HEADER TRUE
      , NULL ''
    );
    RAISE NOTICE 'Imported bronze.erp_px_cat_g1v2 in % ms', EXTRACT(EPOCH FROM (NOW() - start_time));

    RAISE NOTICE 'Loaded in % ms', EXTRACT(EPOCH FROM (NOW() - start_load_time));
  END
$$;

SELECT load_bronze();
