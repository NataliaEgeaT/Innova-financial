SELECT
    expense_id,
    date::timestamp AS expense_ts,
    DATE_TRUNC('day', date::timestamp)::date AS expense_date,
    TRIM(provider) AS provider,
    TRIM(category) AS category,
    amount_usd::numeric(12,2) AS amount_usd,
    TRIM(country) AS country
FROM {{ source('raw', 'expenses') }};
