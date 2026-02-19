-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "9294da4b-f94d-43dc-a353-bb49d0623da0",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "9294da4b-f94d-43dc-a353-bb49d0623da0",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

--What is our total revenue by month?

SELECT 
    d.year,
    d.month,
    d.month_name,
    SUM(f.total_amount) AS total_revenue
FROM fact_sales f
JOIN dim_date d 
    ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Which products generate the highest revenue?

SELECT 
    p.product_id,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY p.product_id
ORDER BY revenue DESC;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Who are our top customers by sales value?

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(f.total_amount) AS total_spent
FROM fact_sales f
JOIN dim_customer c
    ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC;



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--How much revenue comes from each sales channel?

SELECT 
    ch.channel_name,
    ch.channel_type,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_channel ch
    ON f.channel_key = ch.channel_key
GROUP BY ch.channel_name, ch.channel_type
ORDER BY revenue DESC;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Which regions perform the best?

SELECT 
    ch.region,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_channel ch
    ON f.channel_key = ch.channel_key
GROUP BY ch.region
ORDER BY revenue DESC;



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Which product categories are most profitable?

SELECT 
    p.category_id,
    p.category_name,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY p.category_id,p.category_name
ORDER BY revenue DESC;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Which days of the week have highest sales?

SELECT 
    d.day_name,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY d.day_name
ORDER BY revenue DESC;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--How are sales distributed by order status?

SELECT 
    order_status,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS revenue
FROM fact_sales
GROUP BY order_status;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
