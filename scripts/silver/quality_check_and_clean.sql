USE DataWarehouse;

GO
--Quality check in bronze.crm_cust_info before transformation and loading into silver layer
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-- Check for NULLs or Duplicates in Primary Key
SELECT
    cst_id,
    COUNT(*) checking_column
FROM
    bronze.crm_cust_info
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1
    OR cst_id IS NULL;

GO
--Data transformation: clean and remove duplicates
--make rank for each duplicate record based on create date and partition by cst_id
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY
            cst_id
        ORDER BY
            cst_create_date DESC
    ) AS flag_last
FROM
    bronze.crm_cust_info;

--clean data by removing duplicates and null primary key
SELECT
    *
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY
                    cst_create_date DESC
            ) AS flag_last
        FROM
            bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
    ) tmp
WHERE
    flag_last = 1 GO
    -- Check for Unwanted Spaces
    --need to check all string columns
SELECT
    cst_firstname,
    cst_lastname
FROM
    bronze.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname)
    OR cst_lastname != TRIM(cst_lastname);

GO
--clean unwanted spaces
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY
                    cst_create_date DESC
            ) AS flag_last
        FROM
            bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
    ) tmp
WHERE
    flag_last = 1
    /*
    quality check : check the consistency of values in low cardinality columns
     */
    --data standardization and consistency
SELECT DISTINCT
    cst_gndr
FROM
    bronze.crm_cust_info;

GO
--transformation 
--upper use for lowercase to uppercase 
--case when use for mapping values
--trim use for unwanted spaces
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
        WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE
        WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
        WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY
                    cst_create_date DESC
            ) AS flag_last
        FROM
            bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
    ) tmp
WHERE
    flag_last = 1;

GO
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
--Quality check for bronze.crm_prd_info before transformation and loading into silver layer
---------------------------------------------------------------
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info;

GO
-- Check for NULLs or Duplicates in prd_id
SELECT
    prd_id,
    COUNT(*) checking_column
FROM
    bronze.crm_prd_info
GROUP BY
    prd_id
HAVING
    COUNT(*) > 1
    OR prd_id IS NULL;

GO
--create substring for category id from prd_key
SELECT
    prd_id,
    prd_key,
    SUBSTRING(prd_key, 1, 5) AS category_id,
    prd_nm,
    prd_cost,
    prd_line prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info;

GO
/*
create category_id by substring 1 to 5 from prd_key cause join with bronze.erp_px_cat_g1v2 table
--also replace - with _ in category_id cause in bronze.erp_px_cat_g1v2 table category_id contain _ instead of - 
 */
SELECT
    prd_id,
    prd_key,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
    prd_nm,
    prd_cost,
    prd_line prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info;

GO
--check if all category_id in bronze.crm_prd_info exist in bronze.erp_px_cat_g1v2
SELECT
    prd_id,
    prd_key,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
    prd_nm,
    prd_cost,
    prd_line prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info
WHERE
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') NOT in (
        SELECT distinct
            id
        FROM
            bronze.erp_px_cat_g1v2
    );

GO
/*
--Now substract product key from prd_key for join with bronze.crm_sales_detail table
--create product_key by substring from 7 to len(prd_key) from prd_key
 */
SELECT
    prd_id,
    prd_key,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
    SUBSTRING(prd_key, 7, LEN (prd_key)) AS product_key,
    prd_nm,
    prd_cost,
    prd_line prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info;

GO
--check if all product_key in bronze.crm_prd_info exist in bronze.crm_sales_details
SELECT
    prd_id,
    prd_key,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
    SUBSTRING(prd_key, 7, LEN (prd_key)) AS product_key,
    prd_nm,
    prd_cost,
    prd_line prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info
WHERE
    SUBSTRING(prd_key, 7, LEN (prd_key)) NOT in (
        SELECT distinct
            sls_prd_key
        FROM
            bronze.crm_sales_details
    );

GO
--check unwanted spaces in prd_nm
SELECT
    prd_nm
FROM
    bronze.crm_prd_info
WHERE
    prd_nm != TRIM(prd_nm);

GO
--Quality check in column prd_cost for nulls or negative values
SELECT
    prd_cost
FROM
    bronze.crm_prd_info
WHERE
    prd_cost < 0
    OR prd_cost IS NULL;

GO
--adjust null values in prd_cost with 0
SELECT
    prd_id,
    prd_key,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
    SUBSTRING(prd_key, 7, LEN (prd_key)) AS product_key,
    prd_nm,
    ISNULL (prd_cost, 0) AS prd_cost,
    prd_line prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info;

GO
--check prd_line for data standardization and consistency
SELECT DISTINCT
    prd_line
FROM
    bronze.crm_prd_info;

GO
--replace null values in prd_line with 'n/a' and other transformation
SELECT
    prd_id,
    prd_key,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
    SUBSTRING(prd_key, 7, LEN (prd_key)) AS product_key,
    prd_nm,
    ISNULL (prd_cost, 0) AS prd_cost,
    CASE
        when upper(trim(prd_line)) = 'M' THEN 'Mountain'
        when upper(trim(prd_line)) = 'R' THEN 'Road'
        when upper(trim(prd_line)) = 'T' THEN 'Touring'
        when upper(trim(prd_line)) = 'S' THEN 'Other Sales'
        WHEN prd_line IS NULL THEN 'n/a'
        ELSE prd_line
    END AS prd_line,
    prd_start_dt,
    prd_end_dt
FROM
    bronze.crm_prd_info;

GO
--check prd_start_dt and prd_end_dt for invalid date orders (start date < end date)
SELECT
    *
FROM
    bronze.crm_prd_info
WHERE
    prd_end_dt < prd_start_dt;

--end date must  be greater than start date
GO
/*
solution:
1. switch start date and end date if end date > start date
2. Drive the end date from start date
End date = start date of the next product -1 in the same category

 */
--check the solution
/*
SELECT
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
LEAD(prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt) AS next_start_dt,
prd_end_dt
FROM
bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
GO

 */
-- make change 
-- lead is used to get the start date of the next product in the same category
-- partition by category_id cause end date must be in the same category
SELECT
    prd_id,
    REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN (prd_key)) AS prd_key,
    prd_nm,
    ISNULL (prd_cost, 0) AS prd_cost,
    CASE
        when upper(trim(prd_line)) = 'M' THEN 'Mountain'
        when upper(trim(prd_line)) = 'R' THEN 'Road'
        when upper(trim(prd_line)) = 'T' THEN 'Touring'
        when upper(trim(prd_line)) = 'S' THEN 'Other Sales'
        WHEN prd_line IS NULL THEN 'n/a'
        ELSE prd_line
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD (prd_start_dt) OVER (
            PARTITION BY
                prd_key
            ORDER BY
                prd_start_dt
        ) - 1 AS DATE
    ) AS prd_end_dt
FROM
    bronze.crm_prd_info;

GO
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
--quality check in bronze.crm_sales_details 
-------------------------------------------------------------------------------------
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
    bronze.crm_sales_details;

GO
--check date clumn (negative date value or zero or length is equal  8 if not then transform)
SELECT
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM
    bronze.crm_sales_details
WHERE
    sls_order_dt <= 0
    OR LEN (sls_order_dt) != 8
    OR sls_ship_dt <= 0
    OR LEN (sls_ship_dt) != 8
    OR sls_due_dt <= 0
    OR LEN (sls_due_dt) != 8 GO
    --replace zero or negative date value with null and length not equal 8 with null
    --also transform to date formate YYYY-MM-DD
    --CAST(CAST(sls_order_dt AS varchar) AS DATE) use to convert int to date
    --first convert int to varchar then convert varchar to date 
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
        WHEN sls_order_dt <= 8
        or LEN (sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt <= 8
        or LEN (sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt as varchar) as date)
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt <= 8
        or LEN (sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt as varchar) as date)
    END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM
    bronze.crm_sales_details;

GO
--order date must be earlier than ship date and due date
--also ship date must be earlier than due date
--check invalid date orders
SELECT
    *
FROM
    bronze.crm_sales_details
WHERE
    sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

GO
--check data consistency between sales,quantity and price
/*
-business rules :
sum of sales = quantity * price
negative or zero values not allowed in sales, quantity and price
 */
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM
    bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales <= 0
    OR sls_sales IS NULL
    OR sls_quantity <= 0
    OR sls_quantity IS NULL
    OR sls_price <= 0
    OR sls_price IS NULL
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price;

GO
/*
Rules for Transformation:
1.if sales is negative or zero or null then derive it using quantity and price
2. if price is negative or zero or null then derive it using sales and quantity
3. if quantity is negative or zero or null then derive it using sales and price
4. if all three values are negative or zero or null then set them to null
5.if price is negative then conver it to positive
 */
SELECT DISTINCT
    sls_sales as old_sls_sales,
    sls_quantity,
    sls_price as old_sls_price,
    CASE
        WHEN sls_sales IS NULL
        or sls_sales <= 0
        or sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    CASE
        WHEN sls_price IS NULL
        or sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE ABS(sls_price)
    END AS sls_price
FROM
    bronze.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales <= 0
    OR sls_sales IS NULL
    OR sls_quantity <= 0
    OR sls_quantity IS NULL
    OR sls_price <= 0
    OR sls_price IS NULL
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price;

GO
---------------------------------------------------------------------------------------------------------
--Quality check of bronze.erp_cust_az12 before transformation and loading into silver layer
-----------------------------------------------------------------------------------------------
--here cid start with some unnecessary word that is not in crm.customer_info table.so we nee to change it
--also check unwanted spaces in cid 
SELECT
    cid,
    bdate,
    gen
FROM
    bronze.erp_cust_az12
WHERE
    cid LIKE 'NAS%';

GO
--transform Cid
SELECT
    cid,
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN (cid))
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM
    bronze.erp_cust_az12 GO
    --check bdate for out of range dates
SELECT
    bdate
FROM
    bronze.erp_cust_az12
WHERE
    bdate < '1924-01-01' --check that birthdate not less than 100 years
    OR bdate > GETDATE ();

--birthdate not greater than today
GO
-- replace all this out of range dates with null 
SELECT
    cid,
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN (cid))
        ELSE cid
    END AS cid,
    CASE
        WHEN bdate > GETDATE () THEN NULL
        ELSE bdate
    END AS bdate,
    gen
FROM
    bronze.erp_cust_az12;

GO
--check gen for data standardization and consistency
SELECT DISTINCT
    gen,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'n/a'
    END AS gen
FROM
    bronze.erp_cust_az12;

GO
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
--check quality of bronze.erp_loc_a101 before transformation and loading into silver layer
----------------------------------------------------------------------------------------------- 
--there is symbole -  in cid column that is not in crm.cust_info table.so we need to remove it
SELECT
    cid,
    cntry
FROM
    bronze.erp_loc_a101;

GO
--transform cid by removing - symbol
SELECT
    cid,
    REPLACE (cid, '-', '') AS cid
FROM
    bronze.erp_loc_a101;

GO
-- data standardization and consistency in cntry column
SELECT DISTINCT
    cntry
FROM
    bronze.erp_loc_a101
GROUP BY
    cntry;

GO
-- transform cntry column by upper case and trim unwanted spaces and so on
SELECT distinct
    cntry,
    case
        when TRIM(cntry) = 'DE' then 'Germany'
        when TRIM(cntry) in ('US', 'USA') then 'United States'
        when TRIM(cntry) = ' '
        or TRIM(cntry) is null then 'n/a'
        else TRIM(cntry)
    END AS cntry
FROM
    bronze.erp_loc_a101
GROUP BY
    cntry;

GO
-- ====================================================================
--quality check in bronze.erp_px_cat_g1v2 before transformation and loading into silver layer
-- ====================================================================
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM
    bronze.erp_px_cat_g1v2;

GO
--check unwanted spaces in cat, subcat and maintenance columns
SELECT
    *
FROM
    bronze.erp_px_cat_g1v2
WHERE
    TRIM(cat) != cat
    OR TRIM(subcat) != subcat
    OR TRIM(maintenance) != maintenance;

GO
--check data standardization and consistency in cat, subcat and maintenance columns
SELECT DISTINCT
    cat,
    subcat,
    maintenance
FROM
    bronze.erp_px_cat_g1v2;

GO
-- no issue in this data set .so we insert all data into silver layer
