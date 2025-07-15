MODEL (
  name silver.crm_cust_info,
  kind FULL
);

SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname) AS cst_lastname,
  CASE UPPER(TRIM(cst_marital_status))
    WHEN 'M' THEN 'Married'
    WHEN 'S' THEN 'Single'
    ELSE 'Unknown'
  END AS cst_marital_status,
  CASE UPPER(TRIM(cst_gndr))
    WHEN 'M' THEN 'Male'
    WHEN 'F' THEN 'Female'
    ELSE 'Unknown'
  END AS cst_gndr,
  cst_create_date
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
  FROM bronze.crm_cust_info
  WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;