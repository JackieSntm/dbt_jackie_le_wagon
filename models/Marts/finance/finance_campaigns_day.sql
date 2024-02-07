SELECT
f.*,
c.ads_cost,
ROUND((f.operational_margin - c.ads_cost),2) AS ads_margin,
c.ads_click,
c.ads_impression
FROM {{ ref('finance_days') }} AS f
LEFT JOIN {{ ref('int_campaigns_day') }} AS c
USING(date_date)