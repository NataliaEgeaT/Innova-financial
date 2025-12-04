import os
import duckdb

RAW_DIR = "data/raw"
DB_PATH = "warehouse.duckdb"


def get_connection():
    """Devuelve una conexión a DuckDB."""
    return duckdb.connect(DB_PATH)


# =====================================================================
#   STAGING
# =====================================================================
def load_to_staging():
    """
    Carga los CSV a tablas de STAGING en DuckDB,
    aplicando la misma lógica que tus modelos stg_*.sql reales.
    """
    conn = get_connection()
    print("\n🚧 Cargando capa STAGING en DuckDB...\n")

    customers_path = os.path.join(RAW_DIR, "customers.csv")
    transactions_path = os.path.join(RAW_DIR, "transactions.csv")
    payments_path = os.path.join(RAW_DIR, "payments.csv")
    expenses_path = os.path.join(RAW_DIR, "expenses.csv")
    employees_path = os.path.join(RAW_DIR, "employees.csv")
    subscriptions_path = os.path.join(RAW_DIR, "subscriptions.csv")

    # -------------------------
    # STAGING: CUSTOMERS
    # -------------------------
    # Basado en stg_customers.sql
    conn.execute(f"""
        CREATE OR REPLACE TABLE stg_customers AS
        WITH raw AS (
            SELECT
                customer_id,
                TRIM(country)               AS country,
                TRIM(acquisition_channel)   AS acquisition_channel,
                TRIM(segment)               AS segment,
                registration_date::TIMESTAMP AS registration_ts
            FROM read_csv_auto('{customers_path}', HEADER=TRUE)
        )
        SELECT
            customer_id,
            country,
            acquisition_channel,
            segment,
            registration_ts,
            DATE_TRUNC('day', registration_ts)::DATE AS registration_date
        FROM raw;
    """)

    # -------------------------
    # STAGING: TRANSACTIONS
    # -------------------------
    # Basado en stg_transactions.sql
    conn.execute(f"""
        CREATE OR REPLACE TABLE stg_transactions AS
        SELECT
            transaction_id,
            customer_id,
            product_id,
            date::TIMESTAMP        AS transaction_ts,
            DATE_TRUNC('day', date::TIMESTAMP)::DATE AS transaction_date,
            TRIM(country)          AS country,
            quantity::INT          AS quantity,
            unit_price_usd::NUMERIC(12,2) AS unit_price_usd,
            total_usd::NUMERIC(12,2)      AS total_usd
        FROM read_csv_auto('{transactions_path}', HEADER=TRUE);
    """)

    # -------------------------
    # STAGING: PAYMENTS
    # -------------------------
    # Basado en stg_payments.sql
    conn.execute(f"""
        CREATE OR REPLACE TABLE stg_payments AS
        SELECT
            payment_id,
            transaction_id,
            payment_date::TIMESTAMP AS payment_ts,
            DATE_TRUNC('day', payment_date::TIMESTAMP)::DATE AS payment_date,
            TRIM(method)            AS method,
            amount_usd::NUMERIC(12,2) AS amount_usd
        FROM read_csv_auto('{payments_path}', HEADER=TRUE);
    """)

    # -------------------------
    # STAGING: EXPENSES
    # -------------------------
    # Basado en stg_expenses.sql
    conn.execute(f"""
        CREATE OR REPLACE TABLE stg_expenses AS
        SELECT
            expense_id,
            date::TIMESTAMP AS expense_ts,
            DATE_TRUNC('day', date::TIMESTAMP)::DATE AS expense_date,
            TRIM(provider) AS provider,
            TRIM(category) AS category,
            amount_usd::NUMERIC(12,2) AS amount_usd,
            TRIM(country) AS country
        FROM read_csv_auto('{expenses_path}', HEADER=TRUE);
    """)

    # -------------------------
    # STAGING: EMPLOYEES
    # -------------------------
    # Basado en stg_employees.sql
    conn.execute(f"""
        CREATE OR REPLACE TABLE stg_employees AS
        SELECT
            employee_id,
            TRIM(area) AS area,
            salary_usd::NUMERIC(12,2) AS salary_usd,
            TRIM(country) AS country,
            hire_date::TIMESTAMP AS hire_ts,
            DATE_TRUNC('day', hire_date::TIMESTAMP)::DATE AS hire_date
        FROM read_csv_auto('{employees_path}', HEADER=TRUE);
    """)

    # -------------------------
    # STAGING: SUBSCRIPTIONS
    # -------------------------
    # Basado en stg_subscriptions.sql
    conn.execute(f"""
        CREATE OR REPLACE TABLE stg_subscriptions AS
        SELECT
            subscription_id,
            customer_id,
            TRIM(plan) AS plan,
            start_date::TIMESTAMP AS start_ts,
            DATE_TRUNC('day', start_date::TIMESTAMP)::DATE AS start_date,
            end_date::TIMESTAMP   AS end_ts,
            DATE_TRUNC('day', end_date::TIMESTAMP)::DATE AS end_date,
            TRIM(status) AS status,
            monthly_price_usd::NUMERIC(12,2) AS monthly_price_usd
        FROM read_csv_auto('{subscriptions_path}', HEADER=TRUE);
    """)

    print("✔ STAGING cargado correctamente.\n")


# =====================================================================
#   DIMENSIONS
# =====================================================================
def build_dimensions_layer():
    conn = get_connection()
    print("\n🏗 Construyendo DIMENSIONES...\n")

    # -------------------------------------------------
    # DIM_DATE
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        WITH bounds AS (
            SELECT
                MIN(CAST(transaction_date AS DATE)) AS min_date,
                MAX(CAST(transaction_date AS DATE)) AS max_date
            FROM stg_transactions
        ),
        date_spine AS (
            SELECT 
                DATE(gs) AS full_date
            FROM bounds,
            generate_series(
                (SELECT min_date FROM bounds)::TIMESTAMP,
                (SELECT max_date FROM bounds)::TIMESTAMP,
                INTERVAL 1 DAY
            ) AS t(gs)
        )
        SELECT
            CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
            full_date,
            EXTRACT(DAY FROM full_date) AS day,
            EXTRACT(MONTH FROM full_date) AS month,
            EXTRACT(QUARTER FROM full_date) AS quarter,
            EXTRACT(YEAR FROM full_date) AS year,
            STRFTIME(full_date, '%b') AS month_name,
            (EXTRACT(ISODOW FROM full_date) IN (6, 7)) AS is_weekend
        FROM date_spine
        ORDER BY full_date;
    """)

    # -------------------------------------------------
    # DIM_COUNTRY
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE dim_country AS
        WITH union_countries AS (
            SELECT DISTINCT country FROM stg_customers
            UNION
            SELECT DISTINCT country FROM stg_transactions
            UNION
            SELECT DISTINCT country FROM stg_expenses
            UNION
            SELECT DISTINCT country FROM stg_employees
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY country) AS country_key,
            country AS country_name,
            CASE country
                WHEN 'Mexico'        THEN 'MX'
                WHEN 'Panama'        THEN 'PA'
                WHEN 'Costa Rica'    THEN 'CR'
                WHEN 'United States' THEN 'US'
                ELSE 'XX'
            END AS country_code
        FROM union_countries;
    """)

    # -------------------------------------------------
    # DIM_CHANNEL
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE dim_channel AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY acquisition_channel) AS acquisition_channel_key,
            acquisition_channel AS acquisition_channel_name
        FROM (
            SELECT DISTINCT acquisition_channel
            FROM stg_customers
        ) s;
    """)

    # -------------------------------------------------
    # DIM_CUSTOMER (sin customer_name ni status; usa segment)
    #--------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE dim_customer AS
        WITH joined AS (
            SELECT
                c.customer_id,
                c.registration_date,
                c.segment,
                dc.country_key,
                ch.acquisition_channel_key
            FROM stg_customers c
            LEFT JOIN dim_country dc
              ON dc.country_name = c.country
            LEFT JOIN dim_channel ch
              ON ch.acquisition_channel_name = c.acquisition_channel
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
            customer_id,
            country_key,
            acquisition_channel_key,
            segment,
            registration_date
        FROM joined;
    """)

    # -------------------------------------------------
    # DIM_PLAN (solo basado en stg_subscriptions)
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE dim_plan AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY plan) AS plan_key,
            plan                AS plan_name,
            monthly_price_usd   AS plan_price,
            'monthly'           AS billing_period
        FROM (
            SELECT DISTINCT plan, monthly_price_usd
            FROM stg_subscriptions
        ) s;
    """)

    # -------------------------------------------------
    # DIM_EMPLOYEE
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE dim_employee AS
        WITH joined AS (
            SELECT
                e.employee_id,
                e.area,
                e.salary_usd,
                e.hire_date,
                dc.country_key
            FROM stg_employees e
            LEFT JOIN dim_country dc
              ON dc.country_name = e.country
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY employee_id) AS employee_key,
            employee_id,
            country_key,
            salary_usd AS salary,
            area       AS role,
            hire_date  AS start_date
        FROM joined;
    """)

    # -------------------------------------------------
    # DIM_EXPENSE_CATEGORY
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE dim_expense_category AS
        WITH cats AS (
            SELECT DISTINCT category
            FROM stg_expenses
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY category) AS expense_category_key,
            category AS category_name,
            CASE
                WHEN category ILIKE '%marketing%' THEN 'Marketing'
                WHEN category ILIKE '%ads%'       THEN 'Marketing'
                WHEN category ILIKE '%infra%'     THEN 'Infrastructure'
                WHEN category ILIKE '%payroll%'   THEN 'Payroll'
                ELSE 'Operations'
            END AS category_group
        FROM cats;
    """)

    print("✔ DIMENSIONES construidas.\n")


# =====================================================================
#   FACTS
# =====================================================================
def build_facts_layer():
    conn = get_connection()
    print("\n📊 Construyendo TABLAS DE HECHOS...\n")

    # -------------------------------------------------
    # FACT_REVENUE
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE fact_revenue AS
        WITH base AS (
            SELECT
                t.transaction_id,
                t.customer_id,
                t.transaction_date,
                t.country,
                t.total_usd
            FROM stg_transactions t
        ),
        dims AS (
            SELECT
                b.transaction_id,
                d.date_key,
                dc.country_key,
                c.customer_key,
                b.total_usd
            FROM base b
            LEFT JOIN dim_date d
              ON d.full_date = b.transaction_date
            LEFT JOIN dim_country dc
              ON dc.country_name = b.country
            LEFT JOIN dim_customer c
              ON c.customer_id = b.customer_id
        ),
        payments AS (
            SELECT DISTINCT
                transaction_id,
                'Paid' AS payment_status
            FROM stg_payments
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY d.transaction_id) AS revenue_fact_id,
            d.date_key,
            d.customer_key,
            d.country_key,
            d.transaction_id,
            d.total_usd AS amount_usd,
            COALESCE(p.payment_status, 'Pending') AS payment_status
        FROM dims d
        LEFT JOIN payments p
          ON p.transaction_id = d.transaction_id;
    """)

    # -------------------------------------------------
    # FACT_SUBSCRIPTIONS_MRR
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE fact_subscriptions_mrr AS
        WITH sub_base AS (
            SELECT
                s.subscription_id,
                s.customer_id,
                s.plan,
                s.start_date,
                COALESCE(s.end_date, TIMESTAMP '2024-12-31') AS end_date,
                s.status,
                s.monthly_price_usd
            FROM stg_subscriptions s
        ),
        months AS (
            SELECT DISTINCT
                DATE_TRUNC('month', full_date)::DATE AS month_start
            FROM dim_date
        ),
        expanded AS (
            SELECT
                sb.*,
                m.month_start
            FROM sub_base sb
            JOIN months m
              ON m.month_start BETWEEN DATE_TRUNC('month', sb.start_date)
                                   AND DATE_TRUNC('month', sb.end_date)
        ),
        joined AS (
            SELECT
                e.subscription_id,
                e.customer_id,
                e.plan,
                e.month_start,
                e.monthly_price_usd,
                e.status,
                d.date_key AS snapshot_month_key
            FROM expanded e
            JOIN dim_date d
              ON d.full_date = e.month_start
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY subscription_id, snapshot_month_key
            ) AS subscription_fact_id,
            snapshot_month_key AS date_key,
            c.customer_key,
            p.plan_key,
            dc.country_key,
            subscription_id,
            monthly_price_usd AS mrr_value,
            status AS subscription_status
        FROM joined j
        LEFT JOIN dim_customer c
          ON c.customer_id = j.customer_id
        LEFT JOIN dim_plan p
          ON p.plan_name = j.plan
        LEFT JOIN stg_customers sc
          ON sc.customer_id = j.customer_id
        LEFT JOIN dim_country dc
          ON dc.country_name = sc.country;
    """)

    # -------------------------------------------------
    # FACT_EXPENSES
    # -------------------------------------------------
    conn.execute("""
        CREATE OR REPLACE TABLE fact_expenses AS
        WITH joined AS (
            SELECT
                e.expense_id,
                d.date_key,
                dec.expense_category_key,
                dc.country_key,
                e.amount_usd,
                e.provider
            FROM stg_expenses e
            LEFT JOIN dim_date d
              ON d.full_date = e.expense_date
            LEFT JOIN dim_expense_category dec
              ON dec.category_name = e.category
            LEFT JOIN dim_country dc
              ON dc.country_name = e.country
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY expense_id) AS expense_fact_id,
            expense_id,
            date_key,
            expense_category_key,
            country_key,
            amount_usd AS amount,
            provider
        FROM joined;
    """)

    print("✔ HECHOS construidos.\n")
