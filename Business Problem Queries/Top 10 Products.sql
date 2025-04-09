-- Q1: find top 10 highest revenue generating products.

SELECT 
    product_id, SUM(sale_price) AS revenue
FROM
    df_orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;