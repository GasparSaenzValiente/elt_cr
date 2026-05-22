import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.extract_data import ingest_script
from scripts.transform_data import transform

DBT_PROJECT_DIR = os.getenv(
    "DBT_PROJECT_DIR",
    "/opt/airflow/dbt/clash_royale_analytics"
)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

pipeline = DAG(
    dag_id="clash_royale_pipeline",
    description=(
        "End-to-end ELT pipeline: extrae datos de la Clash Royale API, "
        "los carga en MinIO (data lake), los procesa con Spark hacia PostgreSQL "
        "y ejecuta modelos dbt para construir el star schema."
    ),
    start_date=datetime(2025, 8, 2),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["clash-royale", "elt", "spark", "dbt"],
)

with pipeline:
    ingestion_task = PythonOperator(
        task_id="ingestion_task",
        python_callable=ingest_script,
        execution_timeout=timedelta(minutes=30),
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
        op_kwargs={"execution_date": "{{ ds }}"},
        execution_timeout=timedelta(minutes=60),
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --exclude dim_date",
        execution_timeout=timedelta(minutes=20),
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        execution_timeout=timedelta(minutes=10),
    )

    ingestion_task >> transform_task >> dbt_run_task >> dbt_test_task
