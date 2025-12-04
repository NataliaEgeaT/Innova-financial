SELECT
    subscription_id,
    customer_id,
    TRIM(plan) AS plan,
    start_date::timestamp AS start_ts,
    DATE_TRUNC('day', start_date::timestamp)::date AS start_date,
    end_date::timestamp   AS end_ts,
    DATE_TRUNC('day', end_date::timestamp)::date AS end_date,
    TRIM(status) AS status,
    monthly_price_usd::numeric(12,2) AS monthly_price_usd
FROM {{ source('raw', 'subscriptions') }};