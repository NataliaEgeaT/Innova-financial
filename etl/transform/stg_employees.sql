SELECT
    employee_id,
    TRIM(area) AS area,
    salary_usd::numeric(12,2) AS salary_usd,
    TRIM(country) AS country,
    hire_date::timestamp AS hire_ts,
    DATE_TRUNC('day', hire_date::timestamp)::date AS hire_date
FROM {{ source('raw', 'employees') }};
