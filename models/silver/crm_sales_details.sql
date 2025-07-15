MODEL (
  name silver.crm_sales_details,
  kind FULL
);

SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  CASE WHEN sls_order_dt = 0 OR sls_order_dt < 10000000 THEN NULL
       ELSE DATE(sls_order_dt::VARCHAR)
  END AS sls_order_dt,
  CASE WHEN sls_ship_dt = 0 OR sls_ship_dt < 10000000 THEN NULL
       ELSE DATE(sls_ship_dt::VARCHAR)
  END AS sls_ship_dt,
  CASE WHEN sls_due_dt = 0 OR sls_due_dt < 10000000 THEN NULL
       ELSE DATE(sls_due_dt::VARCHAR)
  END AS sls_due_dt,
  CASE
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
      THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,
  sls_quantity,
  CASE
    WHEN sls_price IS NULL OR sls_price <= 0
      THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
  END AS sls_price
FROM bronze.crm_sales_details;