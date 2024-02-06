SELECT 
orders_id
, date_date
, ROUND(SUM(revenue),2) AS revenue_product
, ROUND(SUM(quantity),2) AS qty_product
, ROUND(SUM(purchase_cost),2) AS purchase_cost_product
, ROUND(SUM(margin),2) AS margin_product
FROM {{ ref('int_sales_margin') }}
GROUP BY orders_id, date_date
