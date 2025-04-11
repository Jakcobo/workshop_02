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
from src.load.drive import storing_merged_data


def db_init_task(**kwargs):
    """
    Inicializa la base de datos.
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
    return df.to_json(orient="records")


def transform_spotify_task(**kwargs):
    """
    Transforma los datos de Spotify.
    """
    ti = kwargs['ti']
    spotify_json = ti.xcom_pull(task_ids="read_csv")
    if not spotify_json:
        logging.error("No Spotify data found in XCom.")
        return None
    transformed_json = transform_spotify_data(spotify_json)
    if transformed_json:
        logging.info("Spotify data transformed successfully.")
    return transformed_json


def extract_grammys_task(**kwargs):
    """
    Extrae los datos de la tabla 'grammys' y retorna como JSON.
    """
    df = extract_grammys_data()
    logging.info("Datos extraídos de la tabla 'grammys': %d filas.", len(df))
    return df.to_json(orient="records")


def transform_grammys_task(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='read_db')

    if not raw_data:
        logging.error("No se recibió data desde read_db (XCom es None o vacío).")
        raise ValueError("Fallo en la extracción previa: raw_data está vacío o es None.")

    try:
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)

        df = pd.DataFrame.from_records(raw_data)
        transformed = transform_grammys_data(df)
        return transformed.to_json(orient='records')

    except Exception as e:
        logging.error(f"Error procesando los datos de los Grammy: {str(e)}", exc_info=True)
        raise


def extract_api_task(**kwargs):
    """
    Extrae los datos desde la API (CSV LastFM).
    """
    df = extract_api_data()
    logging.info("Datos extraídos desde la API: %d filas.", len(df))
    return df.to_json(orient="records")


def merge_datasets_task(**kwargs):
    ti = kwargs["ti"]
    spotify_json = ti.xcom_pull(task_ids="transform_csv")
    grammys_json = ti.xcom_pull(task_ids="transform_db")
    api_json = ti.xcom_pull(task_ids="extract")

    if not (spotify_json and grammys_json and api_json):
        logging.error("Faltan datos de uno o más tasks para el merge.")
        return None

    merged_json = merge_datasets(spotify_json, grammys_json, api_json)
    if merged_json:
        logging.info("Merge completado exitosamente.")
    return merged_json


def store_to_drive_task(**kwargs):
    """
    Sube el DataFrame final como CSV a Google Drive.
    """
    ti = kwargs["ti"]
    final_data_json = ti.xcom_pull(task_ids="merge")

    if not final_data_json:
        logging.error("No se recibió data desde merge (XCom vacío).")
        raise ValueError("No data received from merge task.")
    
    df = pd.read_json(final_data_json, orient="records")
    storing_merged_data("merged_dataset.csv", df)
