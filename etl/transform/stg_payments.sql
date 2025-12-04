SELECT
    payment_id,
    transaction_id,
    payment_date::timestamp AS payment_ts,
    DATE_TRUNC('day', payment_date::timestamp)::date AS payment_date,
    TRIM(method)            AS method,
    amount_usd::numeric(12,2) AS amount_usd
FROM {{ source('raw', 'payments') }};
