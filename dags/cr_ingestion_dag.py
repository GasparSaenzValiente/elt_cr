import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.extract_data import ingest_script
from scripts.transform_data import transform

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
    )

    ingestion_task >> transform_task
