WITH base AS (
    SELECT DISTINCT category
    FROM stg_expenses
)
SELECT
    ROW_NUMBER() OVER (ORDER BY category) AS expense_category_key,
    category                              AS category_name,
    CASE
        WHEN category ILIKE '%Marketing%'     THEN 'Marketing'
        WHEN category ILIKE '%Ads%'          THEN 'Marketing'
        WHEN category ILIKE '%Infrastructure%' THEN 'Infrastructure'
        WHEN category ILIKE '%Payroll%'      THEN 'Payroll'
        ELSE 'Operations'
    END AS category_group
FROM base;
