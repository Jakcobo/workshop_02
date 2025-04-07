# airflow/tasks/db_task.py

import os
import sys

# Ajusta el path para poder importar el módulo db.py
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from src.database.db import init_db
from src.extract.extract_spotify import extract_spotify_data
from src.extract.extract_grammys import extract_grammys_data

def db_init_task():
    """
    Función que invoca la inicialización de la base de datos.
    Se utiliza en el DAG como callable en el PythonOperator.
    """
    init_db()

def extract_spotify_task():
    """
    Función que extrae el dataset de Spotify desde el CSV.
    """
    df = extract_spotify_data()
    print("Datos extraídos de Spotify:")
    print(df.head())

def extract_grammys_task():
    """
    Función que extrae los datos de la tabla 'grammys' desde la base de datos.
    """
    df = extract_grammys_data()
    print("Datos extraídos de la tabla 'grammys':")
    print(df.head())