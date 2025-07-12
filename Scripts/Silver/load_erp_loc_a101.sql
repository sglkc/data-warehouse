-- This script tests for duplicate ids, trailing spaces, null and invalid values
-- Inserts cleaned data to table in silver layer

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

-- Find missing customer
SELECT *
FROM bronze.erp_loc_a101
WHERE
  REPLACE(cid, '-', '')
  NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Find invalid country
SELECT *
FROM bronze.erp_loc_a101
WHERE cntry IS NULL;

-- Check data consistency
SELECT *
FROM bronze.erp_loc_a101
WHERE cntry != trim(cntry);
