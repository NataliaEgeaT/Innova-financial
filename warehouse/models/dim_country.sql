WITH union_countries AS (
    SELECT DISTINCT country FROM stg_customers
    UNION
    SELECT DISTINCT country FROM stg_transactions
    UNION
    SELECT DISTINCT country FROM stg_expenses
    UNION
    SELECT DISTINCT country FROM stg_employees
)
SELECT
    ROW_NUMBER() OVER (ORDER BY country)        AS country_key,
    country                                     AS country_name,
    CASE country
        WHEN 'Mexico'        THEN 'MX'
        WHEN 'Panama'        THEN 'PA'
        WHEN 'Costa Rica'    THEN 'CR'
        WHEN 'United States' THEN 'US'
        ELSE 'XX'
    END AS country_code
FROM union_countries;
