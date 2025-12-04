SELECT 
    COUNT(*) AS new_customers_q1_2024
FROM dim_customer c
JOIN dim_date d 
    ON c.created_at = d.full_date
WHERE d.year = 2024
  AND d.quarter = 1;
