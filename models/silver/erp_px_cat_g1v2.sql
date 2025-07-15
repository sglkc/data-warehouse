MODEL (
  name silver.erp_px_cat_g1v2,
  kind FULL
);

SELECT
  TRIM(id) AS id,
  TRIM(cat) AS cat,
  TRIM(subcat) AS subcat,
  TRIM(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2;