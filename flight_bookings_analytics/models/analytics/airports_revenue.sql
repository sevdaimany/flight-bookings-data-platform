{{
  config(
    materialized = 'table',
    )
}}

SELECT 
    d.country,
    SUM(f.amount) AS total_revenue,
    COUNT(*) AS total_bookings,
    AVG(f.amount) AS average_booking_amount

FROM {{ source('gold', 'fact_bookings') }} f
LEFT JOIN {{ source('gold', 'dim_airports') }} d
    ON f.dim_airports_key = d.dim_airports_key
GROUP BY d.country
ORDER BY SUM(f.amount) DESC;