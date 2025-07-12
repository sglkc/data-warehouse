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
  CASE UPPER(TRIM(cst_marital_status))
    WHEN 'M' THEN 'Married'
    WHEN 'S' THEN 'Single'
    ELSE 'Unknown'
  END cst_marital_status,
  CASE UPPER(TRIM(cst_gndr))
    WHEN 'M' THEN 'Male'
    WHEN 'F' THEN 'Female'
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
