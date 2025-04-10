# airflow/dags/dags.py

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from task.task import (
    db_init_task,
    extract_spotify_task,
    extract_grammys_task,
    extract_api_task,
    transform_grammys_task,
    transform_spotify_task,
    merge_datasets_task,
)

default_args = {
    "owner": "jacobo",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    description="DAG para ejecutar ETL: inicialización de DB, extracción de Spotify, Grammys y LastFM Artist Stats",
    schedule_interval="@once", 
) as dag:

    task_db_init = PythonOperator(
        task_id="initialize_db",
        python_callable=db_init_task,
    )

    task_extract_spotify = PythonOperator(
        task_id="extract_spotify",
        python_callable=extract_spotify_task,
    )

    task_transform_spotify = PythonOperator(
        task_id="transform_spotify",
        python_callable=transform_spotify_task,
    )

    task_extract_grammys = PythonOperator(
        task_id="extract_grammys",
        python_callable=extract_grammys_task,
    )

    task_transform_grammys = PythonOperator(
        task_id="transform_grammys",
        python_callable=transform_grammys_task,
    )

    task_extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api_task,
    )

    task_merge = PythonOperator(
        task_id="merge_datasets",
        python_callable=merge_datasets_task,
    )

    task_db_init >> task_extract_spotify >> task_transform_spotify
    task_transform_spotify >> task_extract_grammys >> task_transform_grammys
    task_transform_grammys >> task_extract_api >> task_merge