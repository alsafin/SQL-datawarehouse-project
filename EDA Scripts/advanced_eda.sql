USE DataWarehouse;

GO
-----------------------------change over time analysis-------------------------------------
/*
     analysze how a measure evolves over time .Help track trends and identify seasonality of the data
     */
--analyse sales perfomance over time(year)
/*
     changes over years : a high-level overview insight that helps with strategic decession
     */
SELECT
    YEAR (order_date) AS order_year,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM
    gold.fact_sales
WHERE
    order_date IS NOT NULL
GROUP BY
    YEAR (order_date)
ORDER BY
    YEAR (order_date)
--analyse sales perfomance over time(month)
/*
     --changes over month: details insight to discover seasonality in Data
     */
SELECT
    MONTH (order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM
    gold.fact_sales
WHERE
    order_date IS NOT NULL
GROUP BY
    MONTH (order_date)
ORDER BY
    MONTH (order_date)
--But year and month combination are more understandable
SELECT
    FORMAT (order_date, 'yyyy-MMM') AS order_date,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM
    gold.fact_sales
WHERE
    order_date IS NOT NULL
GROUP BY
    FORMAT (order_date, 'yyyy-MMM')
ORDER BY
    FORMAT (order_date, 'yyyy-MMM')
-----------------------------------cumulative analysis--------------------------------
/*
     cumulative analysis: [window function]([cumulative measure]
     by [date dimension])
     -aggregate the data progressively over the time
     - Helps to understand whether our business is growing or decline over he time
     */
--calculate total sales per month and the running total of sales over time
--this show total running of previous all year
SELECT
    order_date,
    total_sales,
    --window function(default window frame-between unbounded precedding and current row)
    SUM(total_sales) OVER (
        ORDER BY
            order_date
    ) AS running_total_sale
FROM
    (
        SELECT
        DATETRUNC (MONTH, order_date) AS order_date,
        SUM(sales_amount) AS total_sales
    FROM
        gold.fact_sales
    WHERE
            order_date IS NOT NULL
    GROUP BY
            DATETRUNC (MONTH, order_date)
    ) t
--now show running total for yearwise
SELECT
    order_date,
    total_sales,
    --window function(default window frame-between unbounded precedding and current row)
    -- SUM(total_sales) OVER(PARTITION BY order_date ORDER BY order_date) as running_total_sale
    SUM(total_sales) OVER (
        ORDER BY
            order_date
    ) AS running_total_sale,
    AVG(avg_price) OVER (
        ORDER BY
            order_date
    ) AS moving_avg_price
FROM
    (
        SELECT
        DATETRUNC (YEAR, order_date) AS order_date,
        SUM(sales_amount) AS total_sales,
        AVG(price) AS avg_price
    FROM
        gold.fact_sales
    WHERE
            order_date IS NOT NULL
    GROUP BY
            DATETRUNC (YEAR, order_date)
    ) t
GO
-----------------perfomance analysis--------------------------
/*
     -it is the process comparing current value to a target value
     -Help measure success and compare perfomance
     -current[measure]-Target[measure]
     */
--analyze the yearly perfomance of products by comparing each products sales to both its average sales perfomance and the previous year's sales
WITH
    yearly_product_sales
    AS
    (
        SELECT
            YEAR (f.order_date) AS order_year,
            p.product_name,
            SUM(f.sales_amount) AS current_sales
        FROM
            gold.fact_sales f
            LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
        WHERE
            f.order_date IS NOT NULL
        GROUP BY
            YEAR (f.order_date),
            p.product_name
    )
SELECT
    order_year,
    product_name,
    current_sales,
    --compare with avg
    AVG(current_sales) OVER (
        PARTITION BY product_name
    ) AS avg_sales,
    current_sales - AVG(current_sales) OVER (
        PARTITION BY product_name
    ) AS diff_avg,
    -- add flag that clearly describe that it is above or below the avg
    CASE
        WHEN current_sales - AVG(current_sales) OVER (
            PARTITION BY product_name
        ) > 0 THEN 'above avg'
        WHEN current_sales - AVG(current_sales) OVER (
            PARTITION BY product_name
        ) < 0 THEN 'below avg'
        ELSE 'avgPrice'
    END AS avg_change,
    --compare with previous year sales
    LAG (current_sales) OVER (
        PARTITION BY product_name
        ORDER BY
            order_year
    ) AS previous_year_sales,
    --year over year analysis
    current_sales - LAG (current_sales) OVER (
        PARTITION BY product_name
        ORDER BY
            order_year
    ) AS diff_PY_year_sales,
    --add flag for year diff
    CASE
        WHEN current_sales - LAG (current_sales) OVER (
            PARTITION BY product_name
            ORDER BY
                order_year
        ) > 0 THEN 'increasing'
        WHEN current_sales - LAG (current_sales) OVER (
            PARTITION BY product_name
            ORDER BY
                order_year
        ) < 0 THEN 'decreasing'
        ELSE 'No change'
    END AS year_sale_change
FROM
    yearly_product_sales
ORDER BY
    product_name,
    order_year;

--------------part to whole analysis--------------------------------------
/*
 -analysze how an individual part is perfoming compared to the overall . allowing us to understand which category has the greates impact on business
 -([measure]/total[measure])*100 by [dimension]
 */
--which categories contribute the most to overall sales
SELECT
    categorie,
    SUM(sales_amount) AS TotalSales,
    FORMAT (
        SUM(sales_amount) * 100.0 / SUM(SUM(sales_amount)) OVER (),
        'N2'
    ) + '%' AS Percentage_Of_TotalSales
FROM
    gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY
    categorie
ORDER BY
    TotalSales DESC;

----------------------------data segmentation---------------------------------------
/*
 -group the data based on specific range
 -help to understand the correlation between two measure
 -[measure] by [measure]
 */
--segment products into cost range and count how many products fall into each segment
WITH
    product_segment
    AS
    (
        SELECT
            product_key,
            product_name,
            cost,
            CASE
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100
            AND 500 THEN '100-500'
            WHEN cost BETWEEN 500
            AND 1000 THEN '500-1000'
            ELSE 'above 1000'
        END AS cost_range
        FROM
            gold.dim_products
    )
SELECT
    cost_range,
    COUNT(product_key) AS Total_products
FROM
    product_segment
GROUP BY
    cost_range
ORDER BY
    Total_products DESC
GO
--Group customer into three segment based on their spending behavior:
/*
     - VIP: customer with at least 12 month of history and spending more that $5000
     -Regular : customer with at least 12 month of history but spending $5000 or less
     -New : Customers with a lifespan less than 12 month 
     And find the total number of customer by each group
     */
--first step: 
WITH
    customer_spending
    AS
    (
        SELECT
            c.customer_key,
            SUM(f.sales_amount) AS total_spending,
            --to find lifespn we need to first_order date and last_order date
            MIN(order_date) AS first_order,
            MAX(order_date) AS last_order,
            DATEDIFF (MONTH, MIN(order_date), MAX(order_date)) AS life_span_month
        FROM
            gold.fact_sales f
            LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        GROUP BY
            c.customer_key
    )
-- third step: aggregating result
SELECT
    customer_Segment,
    COUNT(customer_key) AS total_customer
FROM
    (
        SELECT
        customer_key,
        --second step:for segmenting customer
        CASE
                WHEN life_span_month >= 12
            AND total_spending <= 5000 THEN 'Regular_customer'
                WHEN life_span_month >= 12
            AND total_spending > 5000 THEN 'VIP_customer'
                ELSE 'new_customer'
            END AS customer_Segment
    FROM
        customer_spending
    ) t
GROUP BY
    customer_Segment
ORDER BY
    total_customer DESC


