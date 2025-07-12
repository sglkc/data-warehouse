CREATE VIEW gold.dim_customers AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY cci.cst_id) customer_key,
    cci.cst_id customer_id,
    cci.cst_key customer_number,
    cci.cst_firstname first_name,
    cci.cst_lastname last_name,
    CASE
      WHEN cci.cst_gndr != 'Unknown' THEN cci.cst_gndr
      ELSE COALESCE(eca.gen, 'Unknown')
    END gender,
    ela.cntry country,
    cci.cst_marital_status marital_status,
    eca.bdate birth_date,
    cci.cst_create_date create_date
  FROM silver.crm_cust_info cci
  JOIN silver.erp_cust_az12 eca
    ON cci.cst_key = eca.cid
  JOIN silver.erp_loc_a101 ela
    ON cci.cst_key = ela.cid
)