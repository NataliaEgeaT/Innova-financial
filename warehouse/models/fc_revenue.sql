WITH base AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.country,
        t.total_usd
    FROM stg_transactions t
),
joined_dims AS (
    SELECT
        b.transaction_id,
        dc.country_key,
        d.date_key,
        c.customer_key,
        b.total_usd
    FROM base b
    LEFT JOIN dim_country dc
        ON dc.country_name = b.country
    LEFT JOIN dim_date d
        ON d.full_date = b.transaction_date
    LEFT JOIN dim_customer c
        ON c.customer_id = b.customer_id
),
payments_status AS (
    SELECT DISTINCT
        transaction_id,
        'Paid' AS status
    FROM stg_payments
)
SELECT
    ROW_NUMBER() OVER (ORDER BY transaction_id) AS revenue_fact_id,
    date_key,
    customer_key,
    dc.country_key,
    transaction_id,
    total_usd      AS amount,
    COALESCE(p.status, 'Pending') AS status
FROM joined_dims dc
LEFT JOIN payments_status p
    ON p.transaction_id = dc.transaction_id;
