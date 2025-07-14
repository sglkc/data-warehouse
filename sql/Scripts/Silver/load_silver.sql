-- Processes and loads cleaned data from bronze to silver layer
-- This function truncates tables before processing

CREATE OR REPLACE FUNCTION load_silver()
RETURNS void
LANGUAGE plpgsql
AS $$
  DECLARE
    start_time TIMESTAMP;
    start_load_time TIMESTAMP;
  BEGIN
    start_load_time = NOW();
    RAISE NOTICE E'Processing data from bronze to silver layer...\n';

    start_time = NOW();
    RAISE NOTICE 'Processing silver.crm_cust_info';
    TRUNCATE silver.crm_cust_info RESTART IDENTITY;
    INSERT INTO silver.crm_cust_info (
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date
    )
    SELECT
      cst_id,
      cst_key,
      TRIM(cst_firstname) cst_firstname,
      TRIM(cst_lastname) cst_lastname,
      CASE UPPER(TRIM(cst_marital_status))
        WHEN 'M' THEN 'Married'
        WHEN 'S' THEN 'Single'
        ELSE 'Unknown'
      END cst_marital_status,
      CASE UPPER(TRIM(cst_gndr))
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'Unknown'
        END cst_gndr,
        cst_create_date
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        ORDER BY flag_last
      )
      WHERE flag_last = 1;
    RAISE NOTICE E'Processed silver.crm_cust_info in % ms\n', EXTRACT(EPOCH FROM (NOW() - start_time)) * 1000;

    start_time = NOW();
    RAISE NOTICE 'Processing silver.crm_prd_info';
    TRUNCATE silver.crm_prd_info RESTART IDENTITY;
    INSERT INTO silver.crm_prd_info (
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
    )
    SELECT
      prd_id,
      REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,
      SUBSTRING(prd_key,7, LENGTH(prd_key)) prd_key,
      prd_nm,
      COALESCE(prd_cost, 0) prd_cost,
      CASE TRIM(UPPER(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'S' THEN 'Other'
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        ELSE 'Unknown'
      END prd_line,
      prd_start_dt,
      LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 prd_end_dt
    FROM bronze.crm_prd_info;
    RAISE NOTICE E'Processed silver.crm_prd_info in % ms\n', EXTRACT(EPOCH FROM (NOW() - start_time)) * 1000;

    start_time = NOW();
    RAISE NOTICE 'Processing silver.crm_sales_details';
    TRUNCATE silver.crm_sales_details RESTART IDENTITY;
    INSERT INTO silver.crm_sales_details (
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
    )
    SELECT
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE
        WHEN sls_order_dt = 0 OR sls_order_dt < 10000000 THEN NULL
        ELSE DATE(sls_order_dt::VARCHAR)
      END AS sls_order_dt,
      CASE
        WHEN sls_ship_dt = 0 OR sls_ship_dt < 10000000 THEN NULL
        ELSE DATE(sls_ship_dt::VARCHAR)
      END AS sls_ship_dt,
      CASE
        WHEN sls_due_dt = 0 OR sls_due_dt < 10000000 THEN NULL
        ELSE DATE(sls_due_dt::VARCHAR)
      END AS sls_due_dt,
      CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
          THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
      END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
      sls_quantity,
      CASE
        WHEN sls_price IS NULL OR sls_price <= 0
          THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price  -- Derive price if original value is invalid
      END AS sls_price
    FROM bronze.crm_sales_details;
    RAISE NOTICE E'Processed silver.crm_sales_details in % ms\n', EXTRACT(EPOCH FROM (NOW() - start_time)) * 1000;

    start_time = NOW();
    RAISE NOTICE 'Processing silver.erp_cust_az12';
    TRUNCATE silver.erp_cust_az12 RESTART IDENTITY;
    INSERT INTO silver.erp_cust_az12 (
      cid,
      bdate,
      gen
    )
    SELECT
      CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
      END cid,
      CASE
        WHEN bdate > now() THEN NULL
        ELSE bdate
      END bdate,
      CASE TRIM(UPPER(gen))
        WHEN 'M' THEN 'Male'
        WHEN 'MALE' THEN 'Male'
        WHEN 'F' THEN 'Female'
        WHEN 'FEMALE' THEN 'Female'
        ELSE 'Unknown'
      END gen
    FROM bronze.erp_cust_az12;
    RAISE NOTICE E'Processed silver.erp_cust_az12 in % ms\n', EXTRACT(EPOCH FROM (NOW() - start_time)) * 1000;

    start_time = NOW();
    RAISE NOTICE 'Processing silver.erp_loc_a101';
    TRUNCATE silver.erp_loc_a101 RESTART IDENTITY;
    INSERT INTO silver.erp_loc_a101 (
      cid,
      cntry
    )
    SELECT
      REPLACE(cid, '-', '') cid,
      CASE
        WHEN cntry IS NULL THEN 'Unknown'
        ELSE TRIM(cntry)
      END cntry
    FROM bronze.erp_loc_a101;
    RAISE NOTICE E'Processed silver.erp_loc_a101 in % ms\n', EXTRACT(EPOCH FROM (NOW() - start_time)) * 1000;

    start_time = NOW();
    RAISE NOTICE 'Processing silver.erp_px_cat_g1v2';
    TRUNCATE silver.erp_px_cat_g1v2 RESTART IDENTITY;
    INSERT INTO silver.erp_px_cat_g1v2 (
      id,
      cat,
      subcat,
      maintenance
    )
    SELECT
      trim(id) id,
      trim(cat) cat,
      trim(subcat) subcat,
      trim(maintenance) maintenance
    FROM bronze.erp_px_cat_g1v2;
    RAISE NOTICE E'Processed silver.erp_px_cat_g1v2 in % ms\n', EXTRACT(EPOCH FROM (NOW() - start_time)) * 1000;

    RAISE NOTICE 'Silver layer loaded in % ms', EXTRACT(EPOCH FROM (NOW() - start_load_time)) * 1000;
  END
$$;

SELECT load_silver();
