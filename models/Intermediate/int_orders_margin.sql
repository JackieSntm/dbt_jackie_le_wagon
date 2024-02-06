WITH int_sales_margin AS (
with 
source_sales as (
    select * from {{ source('raw', 'sales') }}
),
source_product as (
    select 
        *,
        SAFE_CAST(purchse_price AS FLOAT64) AS purchse_price_float64
    from {{ source('raw', 'product') }}
)
select
    s.date_date,
    s.orders_id,
    s.pdt_id as products_id,
    s.revenue,
    s.quantity,
    p.purchse_price_float64 as purchse_price,
    ROUND((s.quantity * p.purchse_price_float64),2) AS purchase_cost,
    ROUND((s.revenue - (s.quantity * p.purchse_price_float64)),2) AS margin
from source_sales s
JOIN source_product p
ON s.pdt_id = p.products_id
)
SELECT 
orders_id
, date_date
, ROUND(SUM(revenue),2) AS revenue_product
, ROUND(SUM(quantity),2) AS qty_product
, ROUND(SUM(purchase_cost),2) AS purchase_cost_product
, ROUND(SUM(margin),2) AS margin_product
FROM int_sales_margin
GROUP BY orders_id, date_date

