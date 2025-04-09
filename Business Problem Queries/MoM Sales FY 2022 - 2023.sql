-- Q3: Find MoM sales comparision FY 2022 and 2023.

WITH Monthly_Revenue AS (SELECT
							MONTH(order_date) AS month_no, YEAR(order_date) AS order_year,
							SUM(sale_price) AS revenue
						FROM df_orders
						WHERE YEAR(order_date) IN (2022, 2023)
						GROUP BY YEAR(order_date), MONTH(order_date)),
                        
Pivoted_Revenue AS (SELECT month_no,
							MAX(CASE WHEN order_year = 2022 THEN revenue END) AS revenue_22,
							MAX(CASE WHEN order_year = 2023 THEN revenue END) AS revenue_23
						FROM Monthly_Revenue GROUP BY month_no)
   
SELECT month_no, revenue_22, revenue_23,
    CONCAT(ROUND(((revenue_23 - revenue_22) / revenue_22) * 100, 2), '%') AS revenue_percentage_MoM,
    CASE
        WHEN (revenue_23 - revenue_22) > 0 THEN '↑'
        WHEN (revenue_23 - revenue_22) < 0 THEN '↓'
        ELSE '→'
    END AS trend
FROM Pivoted_Revenue ORDER BY month_no;








