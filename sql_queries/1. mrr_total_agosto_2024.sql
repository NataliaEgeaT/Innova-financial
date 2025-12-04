SELECT 
    SUM(f.mrr_value) AS total_mrr_aug_2024
FROM fact_subscriptions_mrr f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2024
  AND d.month = 8;
