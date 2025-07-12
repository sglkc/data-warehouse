-- This script tests for duplicate ids, trailing spaces, null and invalid values
-- Inserts cleaned data to table in silver layer

-- Truncate table then insert data to silver
TRUNCATE silver.crm_cust_info RESTART IDENTITY;
INSERT INTO silver.crm_cust_info (
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date
)
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) cst_firstname,
  TRIM(cst_lastname) cst_lastname,
  CASE
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    ELSE 'Unknown'
  END cst_marital_status,
  CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    ELSE 'Unknown'
    END cst_gndr,
    cst_create_date
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    ORDER BY flag_last
  )
  WHERE flag_last = 1;
;

-- Sort duplicated ids by latest timestamp
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info
WHERE cst_id IN (
  SELECT cst_id
  FROM silver.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(cst_id) > 1
)
ORDER BY flag_last
;

-- Get trailing whitespaces
SELECT *
FROM silver.crm_cust_info
WHERE
  cst_firstname != TRIM(cst_firstname)
  OR cst_lastname != TRIM(cst_lastname)
  OR cst_marital_status != TRIM(cst_marital_status)
  OR cst_gndr != TRIM(cst_gndr)
;

-- Check consistency
SELECT cst_gndr, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_gndr;

SELECT cst_marital_status, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_marital_status;