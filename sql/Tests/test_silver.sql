-- This script tests for duplicate ids, trailing spaces, null and invalid values

-- ============================================================
-- Test Silver Layer for crm_cust_info
-- ============================================================

-- Sort duplicated ids by latest timestamp
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info
WHERE cst_id IN (
  SELECT cst_id
  FROM silver.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(cst_id) > 1
)
ORDER BY flag_last
;

-- Get trailing whitespaces
SELECT *
FROM silver.crm_cust_info
WHERE
  cst_firstname != TRIM(cst_firstname)
  OR cst_lastname != TRIM(cst_lastname)
  OR cst_marital_status != TRIM(cst_marital_status)
  OR cst_gndr != TRIM(cst_gndr)
;

-- Check consistency
SELECT cst_gndr, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_gndr;

SELECT cst_marital_status, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_marital_status;

-- ============================================================
-- Test Silver Layer for crm_prd_info
-- ============================================================

-- Find unlisted product from crm_sales_details (No orders yet)
SELECT *
FROM silver.crm_prd_info
WHERE prd_key NOT IN (SELECT DISTINCT sls_prd_key FROM silver.crm_sales_details);

-- Find unlisted category from erp_px_cat_g1v2 (Bad!)
SELECT *
FROM silver.crm_prd_info
WHERE cat_id NOT IN (SELECT DISTINCT id FROM silver.erp_px_cat_g1v2);

-- Get trailing whitespaces
SELECT *
FROM silver.crm_prd_info
WHERE
  prd_key != TRIM(prd_key)
  OR prd_nm != TRIM(prd_nm)
  OR prd_line != TRIM(prd_line)
;

-- Check empty and invalid costs
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check consistency
SELECT prd_line, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_line;

-- Check invalid end date
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- ============================================================
-- Test Silver Layer for crm_sales_details
-- ============================================================

-- Find missing customer
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Find missing product
SELECT *
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- Get trailing whitespaces
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_ord_num != TRIM(sls_ord_num)
  OR sls_prd_key != TRIM(sls_prd_key)
;

-- Find missing customer
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Find missing product
SELECT *
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- Get trailing whitespaces
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_ord_num != TRIM(sls_ord_num)
  OR sls_prd_key != TRIM(sls_prd_key)
;

-- Check empty and invalid numbers
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_sales < 0 OR sls_sales IS NULL
  OR sls_quantity < 0 OR sls_quantity IS NULL
  OR sls_price < 0 OR sls_price IS NULL;

SELECT *
FROM silver.crm_sales_details
WHERE
  sls_cust_id IS NULL
  OR sls_prd_key IS NULL
  OR sls_order_dt IS NULL
  OR sls_ship_dt IS NULL
  OR sls_due_dt IS NULL;

-- Check invalid dates
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_order_dt <= 0
  OR sls_ship_dt <= 0
  OR sls_due_dt <= 0
  OR sls_order_dt > sls_ship_dt
  OR sls_ship_dt > sls_due_dt
  OR sls_due_dt < sls_order_dt;
-- Check empty and invalid numbers
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_sales < 0 OR sls_sales IS NULL
  OR sls_quantity < 0 OR sls_quantity IS NULL
  OR sls_price < 0 OR sls_price IS NULL;

SELECT *
FROM silver.crm_sales_details
WHERE
  sls_cust_id IS NULL
  OR sls_prd_key IS NULL
  OR sls_order_dt IS NULL
  OR sls_ship_dt IS NULL
  OR sls_due_dt IS NULL;

-- Check invalid dates
SELECT *
FROM silver.crm_sales_details
WHERE
  sls_order_dt <= 0
  OR sls_ship_dt <= 0
  OR sls_due_dt <= 0
  OR sls_order_dt > sls_ship_dt
  OR sls_ship_dt > sls_due_dt
  OR sls_due_dt < sls_order_dt;

-- ============================================================
-- Test Silver Layer for erp_cust_az12
-- ============================================================

-- Find missing customer
SELECT *
FROM silver.erp_cust_az12
WHERE
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END
  NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Find invalid date
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > now() OR bdate < '1920-01-01'
ORDER BY bdate;

-- Check data consistency
SELECT gen, COUNT(*)
FROM silver.erp_cust_az12
GROUP BY gen;

-- ===========================================================
-- Test Silver Layer for erp_loc_a101
-- ===========================================================

-- Find missing customer
SELECT *
FROM silver.erp_loc_a101
WHERE
  REPLACE(cid, '-', '')
  NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Find invalid country
SELECT *
FROM silver.erp_loc_a101
WHERE cntry IS NULL;

-- Check data consistency
SELECT *
FROM silver.erp_loc_a101
WHERE cntry != trim(cntry);

-- ============================================================
-- Test Silver Layer for erp_px_cat_g1v2
-- ============================================================

-- Find missing category
SELECT *
FROM silver.crm_prd_info
WHERE
  cat_id
  NOT IN (SELECT DISTINCT id FROM silver.erp_px_cat_g1v2);

-- Find trailing spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Check data consistency
SELECT cat, count(*)
FROM silver.erp_px_cat_g1v2
GROUP BY cat;

SELECT subcat, count(*)
FROM silver.erp_px_cat_g1v2
GROUP BY subcat;

SELECT maintenance, count(*)
FROM silver.erp_px_cat_g1v2
GROUP BY maintenance;
