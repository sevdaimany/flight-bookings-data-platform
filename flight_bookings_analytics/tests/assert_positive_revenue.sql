SELECT *
FROM {{ ref('airports_revenue') }}
WHERE total_revenue < 0