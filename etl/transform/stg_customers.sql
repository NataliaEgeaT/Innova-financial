WITH raw AS (
    SELECT
        customer_id,
        TRIM(country)               AS country,
        TRIM(acquisition_channel)   AS acquisition_channel,
        TRIM(segment)               AS segment,
        registration_date::timestamp AS registration_ts
    FROM {{ source('raw', 'customers') }}
)
SELECT
    customer_id,
    country,
    acquisition_channel,
    segment,
    registration_ts,
    DATE_TRUNC('day', registration_ts)::date AS registration_date
FROM raw;
