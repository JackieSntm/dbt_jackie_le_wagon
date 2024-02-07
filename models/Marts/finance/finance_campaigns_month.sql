WITH month_group AS(
SELECT
*,
CAST(CONCAT(EXTRACT(YEAR FROM date_date), "-", EXTRACT(MONTH FROM date_date),"-", "01") AS date) AS date_month
FROM{{ ref('finance_campaigns_day') }}
)
SELECT

date_month
, SUM(ads_margin) AS ads_margin
, SUM(average_basket) AS average_basket
, SUM(operational_margin) AS operational_margin
, sum(ads_click) AS ads_click
FROM month_group
GROUP BY date_month