WITH base AS (
    SELECT
        c.customer_id,
        c.country,
        c.acquisition_channel,
        c.registration_date,
        c.segment
    FROM stg_customers c
),
joined AS (
    SELECT
        b.customer_id,
        dc.country_key,
        ch.acquisition_channel_key,
        b.registration_date,
        b.segment
    FROM base b
    LEFT JOIN dim_country dc
        ON dc.country_name = b.country
    LEFT JOIN dim_channel ch
        ON ch.acquisition_channel_name = b.acquisition_channel
)
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    country_key,
    acquisition_channel_key,
    registration_date    AS created_at,
    segment              AS customer_status 
FROM joined;
