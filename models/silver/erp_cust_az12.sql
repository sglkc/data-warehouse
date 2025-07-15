MODEL (
  name silver.erp_cust_az12,
  kind FULL
);

SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END AS cid,
  CASE
    WHEN bdate::date > now() THEN NULL
    ELSE bdate::date
  END AS bdate,
  CASE TRIM(UPPER(gen))
    WHEN 'M' THEN 'Male'
    WHEN 'MALE' THEN 'Male'
    WHEN 'F' THEN 'Female'
    WHEN 'FEMALE' THEN 'Female'
    ELSE 'Unknown'
  END AS gen
FROM bronze.erp_cust_az12;