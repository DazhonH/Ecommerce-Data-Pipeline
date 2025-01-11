--Which department has the highest number of products?
SELECT d.department,
       COUNT(p.product_name) as "Product Count"
FROM departments d
JOIN products p
ON d.department_id = p.department_id
GROUP BY d.department
ORDER BY "Product Count" DESC
LIMIT 5;

--Which aisles have the most numbers of products listed?
SELECT a.aisle,
       COUNT(p.product_name) as "product Count"
FROM aisles a
JOIN products p
ON a.aisle_id = p.aisle_id
GROUP BY a.aisle
ORDER BY "product Count" DESC
LIMIT 5;

--What are the top 10 reordered products and which department is it in?

SELECT p.product_name,
         d.department,
		 count(reordered) as "reordered product"
FROM products p
JOIN departments d
ON p.department_id = d.department_id
JOIN order_products op
ON p.product_id = op.product_id
GROUP BY p.product_name,
           d.department
ORDER BY "reordered product" DESC
LIMIT 5;

-- What products have the most days since prior order.
SELECT p.product_name,
o.days_since_prior_order
FROM orders o,
     order_products op,
	 products p
WHERE p.product_id = op.product_id
AND o.order_id = op.order_id
AND o.days_since_prior_order IS NOT NULL
GROUP BY product_name,
 days_since_prior_order
ORDER BY days_since_prior_order DESC
LIMIT 5;

-- What are the top 5 most popular aisles? Include total numer of orders purchased items.
SELECT a. aisle,
COUNT (o.order_number) as "Total Purchased"
FROM products p,
     aisles a,
	 orders o,
	 order_products op
WHERE a.aisle_id = p.aisle_id
AND p.product_id = op.product_Id
AND o.order_id = op.order_id
GROUP BY a.aisle
ORDER BY "Total Purchased" DESC
LIMIT 5;

SELECT * FROM aisles
LIMIT 5


		
     