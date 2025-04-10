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
from src.transform.transform_spotify import transform_spotify_data 
from src.load.merge import merge_datasets

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


def transform_spotify_task(**kwargs):
    """
    Transforms the Spotify data by cleaning and remapping the track_genre column to a new genre_category.
    It pulls the JSON output from the 'extract_spotify' task via XCom,
    converts it into a DataFrame, applies transformation, and returns the result as JSON.
    """
    ti = kwargs['ti']
    spotify_json = ti.xcom_pull(task_ids="extract_spotify")
    if not spotify_json:
        logging.error("No Spotify data found in XCom.")
        return None
    transformed_json = transform_spotify_data(spotify_json)
    if transformed_json:
        logging.info("Spotify data transformed successfully.")
    return transformed_json

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
    raw_data = ti.xcom_pull(task_ids='extract_grammys')
    df = pd.DataFrame(raw_data)
    transformed = transform_grammys_data(df)
    return transformed.to_json(orient='records')


def extract_api_task(**kwargs):
    """
    Extracts the LastFM Artist Stats CSV and returns the DataFrame serialized as JSON.
    """
    df = extract_api_data()
    logging.info("Extracted LastFM Artist Stats data with %d rows.", len(df))
    return df.to_json(orient="records")


def merge_datasets_task(**kwargs):
    ti = kwargs["ti"]
    # Pull transformed outputs of each dataset from XCom.
    spotify_json = ti.xcom_pull(task_ids="transform_spotify")
    grammys_json = ti.xcom_pull(task_ids="transform_grammys")
    api_json = ti.xcom_pull(task_ids="extract_api")
    
    if not (spotify_json and grammys_json and api_json):
        logging.error("Missing data from one or more source tasks for merging.")
        return None
    
    merged_json = merge_datasets(spotify_json, grammys_json, api_json)
    if merged_json:
        logging.info("Datasets merged successfully.")
    return merged_json