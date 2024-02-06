SELECT
date_date
, COUNT(orders_id) AS nb_transactions
, ROUND(SUM(revenue_product),2) AS revenue
, ROUND((SUM(revenue_product) / COUNT(orders_id)),2) AS average_basket
, ROUND(SUM(margin_product),2) AS margin
, ROUND(SUM(operational_margin),2) AS operational_margin
FROM {{ ref('int_orders_operational') }}
GROUP BY date_date
