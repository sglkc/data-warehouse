-- This script tests for duplicate ids, trailing spaces, null and invalid values

-- Sort duplicated ids by latest timestamp
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IN (
  SELECT cst_id
  FROM bronze.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(cst_id) > 1
)
ORDER BY flag_last
;

-- Get trailing whitespaces
SELECT *
FROM bronze.crm_cust_info
WHERE
  cst_firstname != TRIM(cst_firstname)
  OR cst_lastname != TRIM(cst_lastname)
  OR cst_marital_status != TRIM(cst_marital_status)
  OR cst_gndr != TRIM(cst_gndr)
;

-- Check consistency
SELECT cst_gndr, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_gndr;

SELECT cst_marital_status, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_marital_status;
