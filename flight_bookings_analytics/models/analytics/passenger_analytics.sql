SELECT
    p.passenger_id,
    p.name,
    COUNT(*) AS total_bookings,
    SUM(f.amount) AS total_spent,
    AVG(f.amount) AS average_booking_amount

FROM {{ source('gold', 'fact_bookings') }} f

JOIN {{ source('gold', 'dim_passengers') }} p
    ON f.dim_passengers_key = p.dim_passengers_key

GROUP BY
    p.passenger_id,
    p.name;