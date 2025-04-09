# airflow/dags/dags.py

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajustar el sys.path para importar desde la raíz del proyecto (dos niveles arriba)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from task.task import (
    db_init_task,
    extract_spotify_task,
    extract_grammys_task,
    extract_api_task,
    transform_grammys_task,
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
    schedule_interval="@once",  # Ejecuta el DAG una sola vez; ajustar según convenga
    catchup=False,
) as dag:

    task_db_init = PythonOperator(
        task_id="initialize_db",
        python_callable=db_init_task,
    )

    task_extract_spotify = PythonOperator(
        task_id="extract_spotify",
        python_callable=extract_spotify_task,
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

    # Definir la secuencia de ejecución
    task_db_init >> task_extract_spotify >> task_extract_grammys >> task_transform_grammys >> task_extract_api