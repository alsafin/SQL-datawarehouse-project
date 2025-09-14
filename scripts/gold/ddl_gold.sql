/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
USE DataWarehouse;

GO
-- create dimension table of customer 
/*
=======================STEP=======================
-must be check there is no duplicate data in this joinning
SELECT
    customer_id,
    COUNT(*) AS duplicate_count
FROM
    (
        SELECT
        ci.cst_id as customer_id,
        ci.cst_key as customer_name,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        ci.cst_marital_status as marital_status,
        CASE
                WHEN ci.cst_gndr != 'n/a' THEN CI.cst_gndr --CRM IS MASTER TABLE
                ELSE COALESCE(ca.gen, 'n/a')
            END AS gender, --this new gen column is more better than crm adn erp gender column
        ci.cst_create_date as create_date,
        ca.bdate as birth_date,
        la.cntry as country
    FROM
        silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca on ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 la on ci.cst_key = la.cid
    ) AS sub
GROUP BY
    customer_id

HAVING
    COUNT(*) > 1



-give new name in column name 

-surogated key is a system generated unique identifer assign to each record in a table 
-give object name by using view  dim_table

*/
  
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW
    gold.dim_customers AS
SELECT
    ROW_NUMBER() over (
        ORDER BY
            ci.cst_id
    ) as customer_key, --surogated key is a system generated unique identifer assign to each record in a table 
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN CI.cst_gndr --CRM IS MASTER TABLE
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender, --this new gen column is more better than crm adn erp gender column
    ca.bdate as birth_date,
    ci.cst_create_date as create_date
FROM
    silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca on ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la on ci.cst_key = la.cid

GO

-------------------------------------END CUSTOMERS------------------------------------------------------------

--create dimesion table of products 
/*
===========================STEP=================================================
-if prd_end_date is null then it is current info of the product.we take current product we need to filter the data using where
-join this table with product categories table
-check duplicate or uniqueness of that data by this query

SELECT
    prd_key , count(*)
FROM
    (
        SELECT
        pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
    FROM
        silver.crm_prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
    WHERE
    pn.prd_end_dt IS NULL -- if data is null then it is current info of the product.to take current product we need to filter the data using where
    )t
GROUP BY prd_key
HAVING count(*) >1
- rearrange column for better show
-rename column for better readibility
-give surrogated key

*/
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products as 
SELECT
    ROW_NUMBER() Over(order by pn.prd_start_dt,pn.prd_id) as product_key,
    pn.prd_id AS product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as categorie_id,
    pc.cat as categorie,
    pc.subcat  AS sub_categorie,
    pc.maintenance ,
    pn.prd_cost AS cost,
    pn.prd_line AS poduct_line,
    pn.prd_start_dt AS start_date 
FROM
    silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE
    pn.prd_end_dt IS NULL -- if data is null then it is current info of the product.to take current product we need to filter the data using where
GO
------------------------------------------END PRODUCTS---------------------------------------------------------

--create fact sales table

--------------------STEP--------------------------
/*
- building fact: use the dimension surroget key as original keys to connect with the fact table
--connect and remove prd_key and cust_key and replace with product_key and customer_key that i connect with dimension table 
*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales as 
SELECT
sd.sls_ord_num as order_number,
pr.product_key,
c.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price

FROM silver.crm_sales_details as sd
LEFT JOIN gold.dim_products pr on sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers c on sd.sls_cust_id = c.customer_id
