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
