WITH op_costs AS (
    SELECT 
        ma.*,
        CAST(shipping_fee AS FLOAT64) AS shipping_fee_numeric,
        CAST(logCost AS FLOAT64) AS logCost_numeric,
        CAST(ship_cost AS FLOAT64) AS ship_cost_numeric
    FROM {{ ref('int_orders_margin') }} AS ma
    JOIN {{ ref('stg_raw__ship') }} USING (orders_id))
SELECT
    op_costs.orders_id,
    op_costs.date_date,
    op_costs.revenue_product,
    op_costs.margin_product,
    ROUND(margin_product - logCost_numeric - ship_cost_numeric + shipping_fee_numeric, 2) AS operational_margin,
    qty_product
FROM op_costs