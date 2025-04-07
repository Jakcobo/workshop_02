# airflow/dags/db_dag.py

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator

# Ajustar el path para que podamos importar el módulo de tasks
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from task.task import db_init_task, extract_spotify_task, extract_grammys_task

default_args = {
    'owner': 'jacobo',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    description='DAG para inicializar la base de datos PostgreSQL usando SQLAlchemy',
    schedule_interval='@once',  
    catchup=False
) as dag:

    task_db_init = PythonOperator(
        task_id='initialize_db',
        python_callable=db_init_task
    )

    task_extract_spotify = PythonOperator(
        task_id='extract_spotify',
        python_callable=extract_spotify_task
    )

    task_extract_grammys = PythonOperator(
        task_id='extract_grammys',
        python_callable=extract_grammys_task
    )

    # Secuencia: primero se inicializa la DB, luego se extraen los datos de Spotify
    task_db_init >> task_extract_spotify >> task_extract_grammys