/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
--quality check in silver.crm_cust_info after transformation and loading from bronze layer
-------------------------------------------------------------------------------------   
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results


use DataWarehouse;

GO
SELECT
    cst_id,
    COUNT(*)
FROM
    silver.crm_cust_info
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1
    OR cst_id IS NULL;

--Data transformation: clean and remove duplicates
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY
            cst_id
        ORDER BY
            cst_create_date DESC
    ) AS flag_last
FROM
    silver.crm_cust_info;

GO
-- Check for Unwanted Spaces
--need to check all string columns
SELECT
    cst_firstname,
    cst_lastname
FROM
    silver.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname)
    OR cst_lastname != TRIM(cst_lastname);

GO
-- Data Standardization & Consistency
SELECT DISTINCT
    cst_marital_status
FROM
    silver.crm_cust_info;

SELECT DISTINCT
    cst_gndr
FROM
    silver.crm_cust_info;

GO
--final clean data
SELECT
    *
FROM
    silver.crm_cust_info;
GO

--------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--quality check in silver.crm_prd_info after transformation and loading from bronze layer
-------------------------------------------------------------------------------------   
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM
    silver.crm_prd_info;

-- Check for NULLs or Duplicates in prd_id
SELECT
    prd_id,
    COUNT(*) checking_column
FROM
    silver.crm_prd_info
GROUP BY
    prd_id
HAVING
    COUNT(*) > 1
    OR prd_id IS NULL;

GO
--check for unwanted spaces
SELECT
    prd_nm
FROM
    silver.crm_prd_info
WHERE
    prd_nm != TRIM(prd_nm);

--Quality check in column prd_cost for nulls or negative values
SELECT
    prd_cost
FROM
    silver.crm_prd_info
WHERE
    prd_cost < 0
    OR prd_cost IS NULL;

GO
--check prd_line for data standardization and consistency
SELECT DISTINCT
    prd_line
FROM
    silver.crm_prd_info;

GO
--check prd_start_dt and prd_end_dt for invalid date orders (start date < end date)
SELECT
    *
FROM
    silver.crm_prd_info
WHERE
    prd_end_dt < prd_start_dt;

--end date must not be earlier than start date
GO
--check final clean data
SELECT
    *
FROM
    silver.crm_prd_info;
GO

--------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

--check quality in silver.crm_sales_info after transformation and loading from bronze layer
---------------------------------------------------------------------------------------------   
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM
    silver.crm_sales_details;

GO
--check date clumn (negative date value or zero or length is equal  8 if not then transform)
SELECT
    *
FROM
    silver.crm_sales_details
WHERE
    sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

GO
--check data consistency between sales,quantity and price
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM
    silver.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales <= 0
    OR sls_sales IS NULL
    OR sls_quantity <= 0
    OR sls_quantity IS NULL
    OR sls_price <= 0
    OR sls_price IS NULL;

GO
--final check
SELECT
    *
FROM
    silver.crm_sales_details;
GO

--------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--quality check in silver.erp_cust_az12 after transformation and loading from bronze layer
-------------------------------------------------------------------------------------   
--check cid 

SELECT
    cid,
    bdate,
    gen
FROM
    silver.erp_cust_az12
WHERE
    cid LIKE 'NAS%';
GO
--check bdate for out of range dates

SELECT
    bdate
FROM
    silver.erp_cust_az12
WHERE
    bdate < '1924-01-01' --check that birthdate not less than 100 years
    OR bdate > GETDATE(); --birthdate not greater than today
GO

--check gen for data standardization and consistency

SELECT DISTINCT
    gen
FROM
    silver.erp_cust_az12;
GO

---------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------- 
--quality check in silver.erp_loc_a101 after transformation and loading from bronze layer
-------------------------------------------------------------------------------------
--check cid
SELECT
    cid
FROM
    silver.erp_loc_a101;
GO

--check cntry for data standardization and consistency
SELECT DISTINCT
    cntry
FROM
    silver.erp_loc_a101
GROUP BY
    cntry;

GO
