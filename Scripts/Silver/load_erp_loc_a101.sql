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
