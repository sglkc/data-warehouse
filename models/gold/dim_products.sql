MODEL (
  name gold.dim_products,
  kind FULL
);

SELECT
  row_number() OVER (ORDER BY cpi.prd_start_dt, cpi.prd_key) product_key,
  cpi.prd_id product_id,
  cpi.prd_key product_number,
  cpi.prd_nm product_name,
  cpi.cat_id category_id,
  epcgv.cat category,
  epcgv.subcat subcategory,
  epcgv.maintenance,
  cpi.prd_cost cost,
  cpi.prd_line product_line,
  cpi.prd_start_dt start_date,
  cpi.prd_end_dt end_date
FROM silver.crm_prd_info cpi
JOIN silver.erp_px_cat_g1v2 epcgv
  ON cpi.cat_id = epcgv.id
WHERE
  cpi.prd_end_dt IS NULL
