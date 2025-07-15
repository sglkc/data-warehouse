MODEL (
  name silver.erp_loc_a101,
  kind FULL
);

SELECT
  REPLACE(cid, '-', '') AS cid,
  COALESCE(NULLIF(TRIM(cntry), ''), 'Unknown') AS cntry
FROM bronze.erp_loc_a101;