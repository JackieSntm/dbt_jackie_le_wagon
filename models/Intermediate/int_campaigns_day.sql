SELECT 
date_date
, sum(ads_cost) as ads_cost
, sum(impression) as ads_impression
, sum(click) as ads_click
FROM {{ ref('int_campaigns') }}
GROUP BY date_date