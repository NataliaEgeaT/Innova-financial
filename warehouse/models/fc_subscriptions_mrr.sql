WITH sub_base AS (
    SELECT
        s.subscription_id,
        s.customer_id,
        s.plan,
        s.start_date,
        COALESCE(s.end_date, '2024-12-31') AS end_date,
        s.status,
        s.monthly_price_usd
    FROM stg_subscriptions s
),
months AS (
    SELECT DISTINCT
        DATE_TRUNC('month', full_date)::date AS month_start,
        TO_CHAR(DATE_TRUNC('month', full_date), 'YYYYMM01')::int AS snapshot_month_key
    FROM dim_date
),
expanded AS (
    SELECT
        sb.*,
        m.month_start,
        m.snapshot_month_key
    FROM sub_base sb
    JOIN months m
      ON m.month_start BETWEEN DATE_TRUNC('month', sb.start_date)
                           AND DATE_TRUNC('month', sb.end_date)
)
SELECT
    ROW_NUMBER() OVER (ORDER BY subscription_id, snapshot_month_key) AS subscription_fact_id,
    e.snapshot_month_key AS date_key,
    c.customer_key,
    p.plan_key,
    dc.country_key,
    e.subscription_id,
    e.monthly_price_usd AS mrr_value,
    e.status            AS subscription_status
FROM expanded e
LEFT JOIN dim_customer c
    ON c.customer_id = e.customer_id
LEFT JOIN dim_plan p
    ON p.plan_name = e.plan
LEFT JOIN dim_country dc
    ON dc.country_name = (SELECT country FROM stg_customers sc WHERE sc.customer_id = e.customer_id LIMIT 1);
