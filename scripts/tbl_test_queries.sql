/*
===============================================================================
DATA QUALITY TESTING SCRIPT
===============================================================================
This script organizes data validation checks by Schema and Table.
It identifies duplicates, nulls, invalid dates, and referential integrity issues.
*/

-- =============================================================================
-- 1. BRONZE SCHEMA - CRM DATA
-- =============================================================================

-- TABLE: bronze.crm_cust_info
-- Check for Duplicate Customer IDs or NULLs
SELECT cst_id, COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check Gender Distribution
SELECT cst_gndr, COUNT(*) AS bronze_count
FROM bronze.crm_cust_info 
GROUP BY cst_gndr;

-- Detect Leading/Trailing Whitespace in Marital Status
SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

-- TABLE: bronze.crm_sales_details
-- Check for Invalid Date Lengths (Expected 8 characters) or 0 Values
SELECT DISTINCT sls_due_dt, sls_ord_num
FROM bronze.crm_sales_details
WHERE LEN(sls_due_dt) != 8 OR sls_due_dt = '0';

-- Check for Logical Date Errors (Order date cannot be after Ship or Due date)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Count Sales by Product Key (Excluding Key '1')
SELECT sls_prd_key, COUNT(*)
FROM bronze.crm_sales_details
WHERE sls_prd_key > '1'
GROUP BY sls_prd_key;

-- Check Primary/Unique Key Constraints
SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_NAME = 'crm_sales_details' 
  AND TABLE_SCHEMA = 'bronze';


-- =============================================================================
-- 2. BRONZE SCHEMA - ERP DATA
-- =============================================================================

-- TABLE: bronze.erp_cust_az12
-- Identify Invalid Birth Dates (Future dates or pre-1900)
SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate > GETDATE() OR bdate < '1900-01-01' OR bdate IS NULL;

-- Preview Cleaned Data and Identify Orphans (Keys missing from Silver CRM)
SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN UPPER(SUBSTRING(cid, 4, LEN(cid))) ELSE cid END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
         WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
         ELSE 'n/a' END AS gen
FROM bronze.erp_cust_az12 
WHERE CASE WHEN cid LIKE 'NAS%' THEN UPPER(SUBSTRING(cid, 4, LEN(cid))) ELSE cid END 
      NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);


-- =============================================================================
-- 3. SILVER SCHEMA - VALIDATION
-- =============================================================================

-- TABLE: silver.crm_sales_details
-- Verify Calculation Accuracy: (Quantity * Price = Sales) and check for invalid values
SELECT DISTINCT sls_sales, sls_quantity, sls_price    
FROM silver.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- TABLE: silver.erp_cust_az12
-- Check Distinct Genders after cleaning
SELECT DISTINCT gen FROM silver.erp_cust_az12;
 exec silver.load_silver 