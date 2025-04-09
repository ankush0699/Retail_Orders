-- Q4: Which month has highest sales for each category?

WITH monthly_revenue AS (
    SELECT 
        category,
        YEAR(order_date) AS year_no,
        MONTHNAME(order_date) AS month_name,
        SUM(sale_price) AS revenue
    FROM df_orders
    GROUP BY category, year_no, month_name
),
ranked_monthly_revenue AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
    FROM monthly_revenue
)
SELECT category, year_no, month_name, revenue
FROM ranked_monthly_revenue
WHERE rn = 1;

