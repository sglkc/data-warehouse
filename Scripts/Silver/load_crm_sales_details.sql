-- This script tests for duplicate ids, trailing spaces, null and invalid values
-- Inserts cleaned data to table in silver layer

TRUNCATE silver.crm_sales_details RESTART IDENTITY;
INSERT INTO silver.crm_sales_details (
  sls_ord_num
  , sls_prd_key
  , sls_cust_id
  , sls_order_dt
  , sls_ship_dt
  , sls_due_dt
  , sls_sales
  , sls_quantity
  , sls_price
)
SELECT
  TRIM(sls_ord_num) sls_ord_num
  , TRIM(sls_prd_key) sls_prd_key
  , sls_cust_id
  , CASE
    WHEN sls_order_dt = 0 OR sls_order_dt < 1000000 THEN NULL
    ELSE DATE(sls_order_dt::varchar)
  END sls_order_dt -- IF date number IS shorter than date format
  , CASE
    WHEN sls_ship_dt = 0 OR sls_ship_dt < 1000000 THEN NULL
    ELSE DATE(sls_ship_dt::varchar)
  END sls_ship_dt
  , CASE
    WHEN sls_due_dt = 0 OR sls_due_dt < 1000000 THEN NULL
    ELSE DATE(sls_due_dt::varchar)
  END sls_due_dt
  , CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
      THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales -- Recalculate sales if original value is missing or incorrect
  , sls_quantity
  , CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 
      THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price  -- Derive price if original value is invalid
  END AS sls_price
FROM bronze.crm_sales_details;

-- Find missing customer
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Find missing product
SELECT *
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- Get trailing whitespaces
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_ord_num != TRIM(sls_ord_num)
  OR sls_prd_key != TRIM(sls_prd_key)
;

-- Check empty and invalid numbers
SELECT *
FROM silver.crm_sales_details
WHERE 
  sls_sales < 0 OR sls_sales IS NULL
  OR sls_quantity < 0 OR sls_quantity IS NULL
  OR sls_price < 0 OR sls_price IS NULL;

SELECT *
FROM silver.crm_sales_details
WHERE
  sls_cust_id IS NULL
  OR sls_prd_key IS NULL
  OR sls_order_dt IS NULL
  OR sls_ship_dt IS NULL
  OR sls_due_dt IS NULL;

-- Check invalid dates
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_order_dt <= 0
  OR sls_ship_dt <= 0
  OR sls_due_dt <= 0
  OR sls_order_dt > sls_ship_dt
  OR sls_ship_dt > sls_due_dt
  OR sls_due_dt < sls_order_dt;