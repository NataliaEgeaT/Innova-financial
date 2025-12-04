WITH dates AS (
    SELECT
        MIN(transaction_date) AS min_date,
        MAX(transaction_date) AS max_date
    FROM stg_transactions
),
date_spine AS (
    SELECT
        GENERATE_SERIES(min_date, max_date, INTERVAL '1 day')::date AS full_date
    FROM dates
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::int AS date_key,
    full_date,
    EXTRACT(DAY    FROM full_date)::int AS day,
    EXTRACT(MONTH  FROM full_date)::int AS month,
    EXTRACT(QUARTER FROM full_date)::int AS quarter,
    EXTRACT(YEAR   FROM full_date)::int AS year,
    TO_CHAR(full_date, 'Mon') AS month_name,
    EXTRACT(ISODOW FROM full_date) IN (6,7) AS is_weekend
FROM date_spine;
