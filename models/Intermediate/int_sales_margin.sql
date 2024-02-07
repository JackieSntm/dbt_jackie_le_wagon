WIth clean_table_macro AS (
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
    s.pdt_id as product_id,
    s.revenue,
    s.quantity,
    p.purchse_price_float64 as purchse_price,
    ROUND((s.quantity * p.purchse_price_float64),2) AS purchase_cost,
    
from source_sales s
JOIN source_product p
ON s.pdt_id = p.products_id
)
select
*
, {{ margin_percent(revenue, purchase_cost)}} AS margin
FROM clean_table_macro

