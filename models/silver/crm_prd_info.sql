MODEL (
  name silver.crm_prd_info,
  kind FULL
);

SELECT
  prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
  prd_nm,
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE TRIM(UPPER(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'S' THEN 'Other'
    WHEN 'R' THEN 'Road'
    WHEN 'T' THEN 'Touring'
    ELSE 'Unknown'
  END AS prd_line,
  prd_start_dt,
  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
FROM bronze.crm_prd_info;