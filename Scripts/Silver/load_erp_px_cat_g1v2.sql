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

-- Find missing category
SELECT *
FROM silver.crm_prd_info
WHERE
  cat_id
  NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

-- Find trailing spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Check data consistency
SELECT cat, count(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY cat;

SELECT subcat, count(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY subcat;

SELECT maintenance, count(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY maintenance;
