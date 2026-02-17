SELECT SUM(total_monthly_trips)
FROM {{ ref("fct_monthly_zone_revenue") }}
WHERE service_type = 'green'
AND EXTRACT(YEAR FROM revenue_month) = 2019
AND EXTRACT(MONTH FROM revenue_month) = 10