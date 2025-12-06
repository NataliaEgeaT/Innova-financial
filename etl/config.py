import os

IS_AIRFLOW = os.getenv("AIRFLOW_CTX_DAG_ID") is not None

if IS_AIRFLOW:
    DB_PATH = "/opt/airflow/project/warehouse.duckdb"
else:
    DB_PATH = "warehouse_local.duckdb"
