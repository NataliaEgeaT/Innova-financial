WITH joined AS (
    SELECT
        e.employee_id,
        e.area,
        e.salary_usd,
        e.hire_date,
        dc.country_key
    FROM stg_employees e
    LEFT JOIN dim_country dc
        ON dc.country_name = e.country
)
SELECT
    ROW_NUMBER() OVER (ORDER BY employee_id) AS employee_key,
    employee_id,
    area        AS role,
    salary_usd  AS salary,
    country_key,
    hire_date   AS start_date
FROM joined;
