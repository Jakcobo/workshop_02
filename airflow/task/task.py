import os
import sys
import logging
import json
import pandas as pd

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Ajustar el path para llegar a la raíz del proyecto (dos niveles arriba)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Importar funciones desde los módulos en src
from src.database.db import init_db
from src.extract.extract_spotify import extract_spotify_data
from src.extract.extract_grammys import extract_grammys_data
from src.extract.extract_api import extract_api_data
from src.transform.transform_grammys import transform_grammys_data

def db_init_task(**kwargs):
    """
    Inicializa la base de datos.
    Como esta tarea no produce un DataFrame, se retorna un diccionario vacío para mantener la consistencia con XCom.
    """
    init_db()
    logging.info("Base de datos inicializada correctamente.")
    return {} 

def extract_spotify_task(**kwargs):
    """
    Extrae el dataset de Spotify y retorna el DataFrame serializado en JSON.
    """
    df = extract_spotify_data()
    logging.info("Datos extraídos de Spotify: %d filas.", len(df))
    json_data = df.to_json(orient="records")
    return json_data

def extract_grammys_task(**kwargs):
    """
    Extrae los datos de la tabla 'grammys' desde la base de datos y retorna el DataFrame serializado en JSON.
    """
    df = extract_grammys_data()
    logging.info("Datos extraídos de la tabla 'grammys': %d filas.", len(df))
    json_data = df.to_json(orient="records")
    return json_data

def transform_grammys_task(**kwargs):
    """
    Transforms the Grammys data by selecting only the required columns (category, nominee, winner, year)
    and removing rows where nominee is null. It pulls the JSON output from extract_grammys_task via XCom,
    rebuilds the DataFrame, applies the transformation, and returns the transformed DataFrame as JSON.
    """

    ti = kwargs['ti']
    grammys_json = ti.xcom_pull(task_ids='extract_grammys')
    if not grammys_json:
        logging.error("No data found from extract_grammys task.")
        raise ValueError("No data found from extract_grammys task.")
    
    df = pd.read_json(grammys_json, orient="records")
    transformed_df = transform_grammys_data(df)
    logging.info("Transformed Grammys data: %d rows.", len(transformed_df))
    return transformed_df.to_json(orient="records")


def extract_api_task(**kwargs):
    """
    Extracts the LastFM Artist Stats CSV and returns the DataFrame serialized as JSON.
    """
    df = extract_api_data()
    logging.info("Extracted LastFM Artist Stats data with %d rows.", len(df))
    return df.to_json(orient="records")
