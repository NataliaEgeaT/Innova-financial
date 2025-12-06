SELECT 
    COUNT(*) AS new_customers_q1_2024
FROM dim_customer c
JOIN dim_date d 
    ON c.registration_date = d.full_date
WHERE d.year = 2024
  AND d.quarter = 1;
