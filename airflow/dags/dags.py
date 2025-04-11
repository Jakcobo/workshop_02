# airflow/dags/dags.py

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Agrega el path raíz del proyecto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Importación de las funciones de tareas
from task.task import (
    db_init_task,
    extract_spotify_task,
    extract_grammys_task,
    extract_api_task,
    transform_grammys_task,
    transform_spotify_task,
    merge_datasets_task,
    store_to_drive_task
)

# Argumentos por defecto
default_args = {
    "owner": "jacobo",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    description="ETL completo: extrae desde DB, CSV y API; transforma y almacena en Drive",
    schedule_interval="@once",
    catchup=False,
) as dag:

    # 1. Inicializa la base de datos
    task_db_init = PythonOperator(
        task_id="initialize_db",
        python_callable=db_init_task,
    )

    # 2. Extracción desde Spotify (CSV)
    task_extract_spotify = PythonOperator(
        task_id="read_csv",
        python_callable=extract_spotify_task,
    )

    # 3. Transformación de Spotify
    task_transform_spotify = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_spotify_task,
    )

    # 4. Extracción desde la base de datos (Grammys)
    task_extract_grammys = PythonOperator(
        task_id="read_db",
        python_callable=extract_grammys_task,
    )

    # 5. Transformación de datos de Grammys
    task_transform_grammys = PythonOperator(
        task_id="transform_db",
        python_callable=transform_grammys_task,
    )

    # 6. Extracción desde la API (LastFM)
    task_extract_api = PythonOperator(
        task_id="extract",
        python_callable=extract_api_task,
    )

    # 7. Merge de todos los datasets
    task_merge = PythonOperator(
        task_id="merge",
        python_callable=merge_datasets_task,
    )

    # 8. Subida del dataset final a Google Drive
    task_store_drive = PythonOperator(
        task_id="store",
        python_callable=store_to_drive_task,
    )

    # Dependencias según la estructura deseada
    ttask_db_init >> task_extract_grammys 

    task_extract_spotify >> task_transform_spotify
    task_extract_grammys >> task_transform_grammys

    [task_transform_spotify, task_transform_grammys, task_extract_api] >> task_merge
    task_merge >> task_store_drive
