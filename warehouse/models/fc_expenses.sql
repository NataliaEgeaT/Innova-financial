WITH joined AS (
    SELECT
        e.expense_id,
        d.date_key,
        dc.country_key,
        dec.expense_category_key,
        e.amount_usd
    FROM stg_expenses e
    LEFT JOIN dim_date d
        ON d.full_date = e.expense_date
    LEFT JOIN dim_country dc
        ON dc.country_name = e.country
    LEFT JOIN dim_expense_category dec
        ON dec.category_name = e.category
)
SELECT
    ROW_NUMBER() OVER (ORDER BY expense_id) AS expense_fact_id,
    expense_id,
    date_key,
    expense_category_key,
    country_key,
    amount_usd AS amount
FROM joined;
