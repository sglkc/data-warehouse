-- Tests if any relation fails

SELECT *
FROM gold.fact_sales fs
JOIN gold.dim_customers dc
  ON fs.customer_key = dc.customer_key
JOIN gold.dim_products dp
  ON fs.product_key = dp.product_key
WHERE
  dp.product_key IS NULL
  OR dc.customer_key IS NULL;