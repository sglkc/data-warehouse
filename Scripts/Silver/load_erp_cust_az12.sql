-- Inserts cleaned data to table in silver layer
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

