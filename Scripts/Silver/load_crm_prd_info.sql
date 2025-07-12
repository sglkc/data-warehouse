-- This script tests for duplicate ids, trailing spaces, null and invalid values
-- Inserts cleaned data to table in silver layer

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
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id, -- Extract product category for erp_px_cat_g1v2 relation
  SUBSTRING(prd_key,7, LENGTH(prd_key)) prd_key, -- Extract product key for crm_sales_details relation
  prd_nm,
  COALESCE(prd_cost, 0) prd_cost, -- Change null cost to 0
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

-- Find unlisted product from crm_sales_details (No orders yet)
SELECT *
FROM silver.crm_prd_info
WHERE prd_key NOT IN (SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details);

-- Find unlisted category from erp_px_cat_g1v2 (Bad!)
SELECT *
FROM silver.crm_prd_info
WHERE cat_id NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

-- Get trailing whitespaces
SELECT *
FROM silver.crm_prd_info
WHERE
  prd_key != TRIM(prd_key)
  OR prd_nm != TRIM(prd_nm)
  OR prd_line != TRIM(prd_line)
;

-- Check empty and invalid costs
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check consistency
SELECT prd_line, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_line;

-- Check invalid end date
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;
