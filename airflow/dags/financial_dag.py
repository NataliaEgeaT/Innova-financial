from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


from etl.extract import extract_csvs
from etl.load import load_to_staging, build_dimensions_layer, build_facts_layer
from etl.quality import validate_row_counts, validate_nulls


DEFAULT_ARGS = {
    "owner": "Natalia Egea Terraza",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="financial_pipeline",
    description="Pipeline Data Warehouse Financiero",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["finance", "datawarehouse", "etl"],
) as dag:


    with TaskGroup("extract_layer", tooltip="Extracción desde CSV → memoria") as extract_group:
        
        extract_raw = PythonOperator(
            task_id="extract_csv_raw_data",
            python_callable=extract_csvs
        )

    with TaskGroup("staging_layer", tooltip="Carga a tablas staging") as staging_group:

        load_stg = PythonOperator(
            task_id="load_staging_tables",
            python_callable=load_to_staging
        )

    with TaskGroup("dimensions_layer", tooltip="Construcción de dimensiones") as dimensions_group:
        
        build_dims = PythonOperator(
            task_id="build_dimensions",
            python_callable=build_dimensions_layer
        )

    with TaskGroup("facts_layer", tooltip="Construcción de tablas de hechos") as facts_group:

        build_facts = PythonOperator(
            task_id="build_facts",
            python_callable=build_facts_layer
        )

    with TaskGroup("quality_layer", tooltip="Validaciones de calidad") as quality_group:
        
        validate_counts = PythonOperator(
            task_id="validate_row_counts",
            python_callable=validate_row_counts
        )

        validate_nulls_task = PythonOperator(
            task_id="validate_null_values",
            python_callable=validate_nulls
        )

        validate_counts >> validate_nulls_task


    extract_group >> staging_group >> dimensions_group >> facts_group >> quality_group