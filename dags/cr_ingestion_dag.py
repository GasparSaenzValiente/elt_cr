import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.extract_data import ingest_script
from scripts.transform_data import transform

DBT_PROJECT_DIR = "/opt/airflow/dbt/clash_royale_analytics"

ingestion = DAG(
    dag_id="ingest_script",
    start_date=datetime(2025, 8, 2),
    schedule="@daily",
    catchup=False
)

with ingestion:
    ingestion_task = PythonOperator(
        task_id="ingestion_task",
        python_callable=ingest_script
    )
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
        op_kwargs={'execution_date': '{{ ds }}'}
    )
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run"
    )
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test"
    )

    # 5. Define la secuencia COMPLETA del pipeline
    ingestion_task >> transform_task >> dbt_run_task >> dbt_test_task
