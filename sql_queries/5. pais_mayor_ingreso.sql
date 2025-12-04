SELECT 
    co.country_name,
    SUM(r.amount) AS total_revenue_2024
FROM fact_revenue r
JOIN dim_country co ON r.country_key = co.country_key
JOIN dim_date d ON r.date_key = d.date_key
WHERE d.year = 2024
GROUP BY co.country_name
ORDER BY total_revenue_2024 DESC
LIMIT 1;
