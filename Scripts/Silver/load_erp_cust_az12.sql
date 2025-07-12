-- This script tests for duplicate ids, trailing spaces, null and invalid values
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

-- Find missing customer
SELECT *
FROM silver.erp_cust_az12
WHERE
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END
  NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);
  
-- Find invalid date
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > now() OR bdate < '1920-01-01'
ORDER BY bdate;

-- Check data consistency
SELECT gen, COUNT(*)
FROM silver.erp_cust_az12
GROUP BY gen;