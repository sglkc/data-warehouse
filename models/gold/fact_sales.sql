MODEL (
  name gold.fact_sales,
  kind FULL
);

SELECT
  csd.sls_ord_num order_number,
  dp.product_key,
  dc.customer_key,
  csd.sls_order_dt order_date,
  csd.sls_ship_dt ship_date,
  csd.sls_due_dt due_date,
  csd.sls_sales sales,
  csd.sls_quantity quantity,
  csd.sls_price price
FROM silver.crm_sales_details csd
JOIN gold.dim_customers dc
  ON dc.customer_id = csd.sls_cust_id
JOIN gold.dim_products dp
  ON dp.product_number = csd.sls_prd_key
