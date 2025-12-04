WITH marketing AS (
    SELECT SUM(f.amount) AS marketing_spend
    FROM fact_expenses f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_expense_category c 
        ON f.expense_category_key = c.expense_category_key
    WHERE d.year = 2024
      AND c.category_group = 'Marketing'
),
new_customers AS (
    SELECT COUNT(*) AS customer_count
    FROM dim_customer c
    JOIN dim_date d 
        ON c.created_at = d.full_date
    WHERE d.year = 2024
)
SELECT 
    marketing_spend / customer_count AS cac_2024
FROM marketing, new_customers;
