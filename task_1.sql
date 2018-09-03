-- SQL shell (psql)
-- \COPY orders(client_id, purchase_date) FROM 'C:\Users\Neil\Downloads\orders.csv' DELIMITER ',' CSV HEADER;

-- Extract month from purchase date
CREATE TABLE order_month AS
SELECT client_id, 
	DATE_PART('month', purchase_date) AS purchase_month
	FROM public.orders;
	
CREATE TABLE id_month_count AS
SELECT DISTINCT client_id, purchase_month,
		COUNT(order_month.client_id) OVER 
		(PARTITION BY order_month.client_id, order_month.purchase_month) AS purchases_in_month
		FROM public.order_month;
		
-- #1
-- month | new_buyers
--	5	 | 100
--  6	 | 15
SELECT DISTINCT 
	min AS month, 
	COUNT(client_id) OVER 
	(PARTITION BY b.min) AS new_buyers
FROM
	(SELECT DISTINCT client_id,
		MIN(purchase_month) OVER
		(PARTITION BY id_month_count.client_id)
	FROM
	id_month_count) b
ORDER BY month;

-- #2
-- month | buyers
-- 6	 | 79
-- 7 	 | 68
SELECT DISTINCT second_purchase_month AS month,
		COUNT(client_id) OVER
		(PARTITION BY second_purchase_month) AS buyers
FROM
(SELECT DISTINCT id_month_count.client_id, 
				id_month_count.purchase_month AS first_purchase_month,
				b.purchase_month AS second_purchase_month
FROM id_month_count
JOIN (SELECT client_id, purchase_month FROM id_month_count) b
ON id_month_count.client_id = b.client_id
WHERE id_month_count.purchase_month + 1 = b.purchase_month) c
ORDER BY month;

-- #3
-- month | returning_buyers
-- 7	 | 5
-- 8 	 | 1
SELECT DISTINCT second_purchase_month AS month,
		COUNT(client_id) OVER
		(PARTITION BY second_purchase_month) as returning_buyers
FROM
((SELECT DISTINCT id_month_count.client_id,
				b.purchase_month AS second_purchase_month
FROM id_month_count
JOIN (SELECT client_id, purchase_month FROM id_month_count) b
ON id_month_count.client_id = b.client_id
WHERE id_month_count.purchase_month + 1 < b.purchase_month)
EXCEPT
(SELECT DISTINCT id_month_count.client_id,
				b.purchase_month AS second_purchase_month
FROM id_month_count
JOIN (SELECT client_id, purchase_month FROM id_month_count) b
ON id_month_count.client_id = b.client_id
WHERE id_month_count.purchase_month + 1 = b.purchase_month)) c
ORDER BY month;

-- #4
-- Я понимаю этого, как торговые точки, которые заказали что-то 
-- в прошлом месяце, а не заказали ничего в этом месяце.
-- month | buyers_not_returning
-- 6	 | 21
-- 7	 | 26
-- 8	 | 73
-- 9	 | 1
SELECT DISTINCT next_month AS month,
		COUNT(client_id) OVER
		(PARTITION BY next_month) AS buyers_not_returning
FROM 
((SELECT client_id, purchase_month + 1 AS next_month
FROM id_month_count)
EXCEPT
(SELECT client_id, purchase_month AS next_month
FROM id_month_count)) a
ORDER BY month;