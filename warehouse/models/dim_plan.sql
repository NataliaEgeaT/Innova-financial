SELECT
    ROW_NUMBER() OVER (ORDER BY plan) AS plan_key,
    plan                             AS plan_name,
    monthly_price_usd                AS plan_price,
    'monthly'                        AS billing_period
FROM (
    SELECT DISTINCT plan, monthly_price_usd
    FROM stg_subscriptions
) s;
