USE DataWarehouse;

GO

/*
WE NEED TO CHECK DATA INTEGRATION CAUSE 

-THERE IS FIND NULLS BUT WHY THOUGH WE CLEAN DATA
CAUSE NULL OFTEN COME FROM JOIN TABLE ..NULL WILL APPEAR IF SQL FIND NO MATCH 
- AGAIN FIND PROBLEM THAT IN CST_GNDR THERE IS MALE BUT IN GEN THERE IS FEMALE.SO WHAT WE DO?
WE NEED TO TALK THE STACK HOLDER OR DATABASE ENGEENER THAT WHICH SOURCE IS THE MASTER FOR THIS VALUE ? CRM or ERP? IF SAY CRMM THEN CRM HAS THE MORE ACCURATE VALUE ..SO WE TAKE ALL GENDER FROM CST_GENDR BUT IF NOT FIND IN CST_GNDR THEN WE TAKE FROM GEN TABLE

*/

--------------------date integrity customer table-------------------------------
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN CI.cst_gndr --CRM IS MASTER TABLE
        ELSE COALESCE(ca.gen, 'n/a')
    END AS new_gen --this new gen column is more better than crm adn erp gender column
    

    
FROM
    silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca on ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la on ci.cst_key = la.cid
ORDER BY
    1,
    2
GO
-----------------------------data integrity fact table--------------------------------
--foreign key integrity (dimension)

SELECT
*
FROM
gold.fact_sales F
LEFT JOIN gold.dim_customers c on c.customer_key = f.customer_key
LEFT JOIN gold.dim_products pr on pr.product_key = f.product_key
WHERE pr.product_key IS NULL
