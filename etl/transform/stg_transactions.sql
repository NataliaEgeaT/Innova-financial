SELECT
    transaction_id,
    customer_id,
    product_id,
    date::timestamp        AS transaction_ts,
    DATE_TRUNC('day', date::timestamp)::date AS transaction_date,
    TRIM(country)          AS country,
    quantity::int          AS quantity,
    unit_price_usd::numeric(12,2) AS unit_price_usd,
    total_usd::numeric(12,2)      AS total_usd
FROM {{ source('raw', 'transactions') }};