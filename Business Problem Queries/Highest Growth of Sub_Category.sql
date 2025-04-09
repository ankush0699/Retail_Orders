-- Q5: Which sub_cateegory had highest growth by profit in 2023 compare to 2022?

WITH Yearly_Profit AS (SELECT YEAR(order_date) AS order_year, sub_category, SUM(profit) AS profit
						FROM df_orders
						WHERE YEAR(order_date) IN (2022, 2023)
						GROUP BY order_year, sub_category
                        order BY order_year, profit),
                        
Pivoted_Profit AS (SELECT sub_category,
	   MAX(CASE WHEN order_year = 2022 THEN profit END) AS profit_22,
	   MAX(CASE WHEN order_year = 2023 THEN profit END) AS profit_23
	   FROM Yearly_Profit GROUP BY sub_category)
       
SELECT sub_category, profit_22, profit_23, (profit_23 - profit_22) AS profit,
    CASE
        WHEN (profit_23 - profit_22) > 0 THEN '↑'
        WHEN (profit_23 - profit_22) < 0 THEN '↓'
        ELSE '→'
    END AS trend
FROM Pivoted_Profit ORDER BY profit DESC LIMIT 1;
                    
                   