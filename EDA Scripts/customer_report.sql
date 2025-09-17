USE DataWarehouse
GO
-------------------------------Build Customer Report----------------------------------
/*
     ======================================================================================
     customer report
     ======================================================================================
     purpose :
     -this report consolidates key metrcis and behaviours
     HighLights:
     1. Gather essential fields such as names, ages, and transaction details
     2. segments customer into categories (VIP,Regular,New) and age groups
     3. aggregates customer level metrics :
     -Total orders
     -total sales
     -total quantity purchased
     -total prducts
     -lifepan (in month)
     4. calculates valuable KPIs :
     -recency (months since first order)
     - average order value
     -average monthly spend
     ====================================================================================
     */
/*---------------------------------------------------------------------------------
     1)Base Query : Retrives core column from tables
     -----------------------------------------------------------------------------------
     */
IF OBJECT_ID('gold.report_customer', 'V') IS NOT NULL
    DROP VIEW gold.report_customer;
GO
CREATE VIEW gold.report_customer
AS
    WITH
        base_query
        AS
        (
            --base query
            SELECT
                f.order_number,
                f.product_key,
                f.order_date,
                f.sales_amount,
                f.quantity,
                c.customer_key,
                c.customer_number,
                c.first_name,
                c.last_name,

                --create new column
                CONCAT (c.first_name, ' ', c.last_name) AS customer_name,
                c.birth_date,
                --calculate age group
                DATEDIFF (YEAR, c.birth_date, GETDATE ()) AS age


            FROM
                gold.fact_sales f
                LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
            WHERE
            order_date IS NOT NULL
        ),
        --2)this cte represent customer aggregation : summerize key metrics at the customer level
        customer_aggregation
        AS
        (
            SELECT
                customer_key,
                customer_number,
                customer_name,
                age,
                COUNT(DISTINCT order_number) AS total_orders,
                SUM(sales_amount) AS total_sales,
                SUM(sales_amount) AS total_spending,
                SUM(quantity) AS total_quantity,
                COUNT(DISTINCT product_key) AS total_products,
                MAX(order_date) AS last_order,
                DATEDIFF (MONTH, MIN(order_date), MAX(order_date)) AS life_span_month
            FROM
                base_query
            GROUP BY
            customer_key,
            customer_number,
            customer_name,
            age
        )
    --now
    SELECT
        customer_number,
        customer_name,
        age,
        -- age group query
        CASE
        WHEN age < 20 THEN 'under 20'
        WHEN age BETWEEN 20
        AND 29 THEN '20-29'
        WHEN age BETWEEN 30
        AND 39 THEN '30-39'
        WHEN age BETWEEN 40
        AND 49 THEN '40-49'
        ELSE '50 and above'
    END AS age_group,
        --customer segment  query
        CASE
        WHEN life_span_month >= 12
            AND total_spending <= 5000 THEN 'Regular_customer'
        WHEN life_span_month >= 12
            AND total_spending > 5000 THEN 'VIP_customer'
        ELSE 'new_customer'
    END AS customer_Segment,
        total_orders,
        total_sales,
        total_quantity,
        total_products,
        last_order,
        --recency month since first order value
        DATEDIFF(MONTH, last_order, GETDATE()) AS recency,
        life_span_month,
        --compute average order value(avo)
        CASE
        WHEN total_sales = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_value,
        --compute average monthly speend


        CASE
        WHEN life_span_month = 0 THEN 0
        ELSE total_sales / life_span_month
    END AS avg_monthly_spend
    FROM
        customer_aggregation
