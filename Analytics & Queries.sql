-- 1. Total Sales Revenue For Each Store
SELECT
    s.store_id,
    s.city,
    SUM(o.total_amount) AS total_revenue
FROM pizzeria.orders o
JOIN pizzeria.stores s
    ON o.store_id = s.store_id
GROUP BY
    s.store_id,
    s.city
ORDER BY
    total_revenue DESC;

-- 2. Who are the top 10 most valuable customers (by total spending)?
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.total_amount) AS total_spent
FROM pizzeria.customers c
JOIN pizzeria.orders o
    ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name
ORDER BY
    total_spent DESC
LIMIT 10;


--3. What is the most popular menu item (by quantity sold) across all stores?
SELECT 
    mi.item_id,
    mi.name AS menu_item,
    SUM(oi.quantity) AS total_quantity_sold
FROM pizzeria.order_items oi
JOIN pizzeria.menu_items mi 
    ON oi.item_id = mi.item_id
GROUP BY mi.item_id, mi.name
ORDER BY total_quantity_sold DESC
LIMIT 1;


-- 4. 	What is the average order value?
SELECT 
    AVG(order_total) AS average_order_value
FROM (
    SELECT 
        o.order_id,
        SUM(oi.quantity * mi.price) AS order_total
    FROM pizzeria.orders o
    JOIN pizzeria.order_items oi 
        ON o.order_id = oi.order_id
    JOIN pizzeria.menu_items mi
        ON oi.item_id = mi.item_id
    GROUP BY 
        o.order_id
) AS order_totals;


-- 5. What are the busiest hours of the day for orders (e.g., 5 PM, 6 PM)?
SELECT
	EXTRACT(HOUR FROM o.order_timestamp) AS order_hour,
    COUNT(*) AS total_orders
FROM pizzeria.orders o
GROUP BY 
    EXTRACT(HOUR FROM o.order_timestamp)
ORDER BY 
    total_orders DESC;

