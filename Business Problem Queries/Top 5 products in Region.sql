-- Q2: Find top 5 highest selling products in each region.

WITH CTE AS (
    SELECT  
        region, 
        product_id, 
        SUM(sale_price) AS revenue
    FROM df_orders
    GROUP BY region, product_id
    ORDER BY region, revenue DESC
)
SELECT *
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rn
    FROM CTE
) AS ranked_CTE
WHERE rn <= 5;
