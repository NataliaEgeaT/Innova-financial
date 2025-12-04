SELECT
    SUM(r.amount) - SUM(e.amount) AS fcf_dec_2024
FROM dim_date d
LEFT JOIN fact_revenue r ON r.date_key = d.date_key
LEFT JOIN fact_expenses e ON e.date_key = d.date_key
WHERE d.year = 2024
  AND d.month = 12;
