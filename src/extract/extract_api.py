import os
import pandas as pd
import logging

def extract_api_data():
    """
    Extrae los datos del CSV 'lastfm_artist_stats.csv' ubicado en la carpeta 'data'
    y retorna un DataFrame de pandas con los datos extraídos.
    """
    # Se asume que la carpeta 'data' está en la raíz del proyecto.
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    file_path = os.path.join(base_dir, 'data', 'lastfm_artist_stats.csv')
    
    try:
        df = pd.read_csv(file_path)
        logging.info("Datos extraídos correctamente desde %s.", file_path)
        logging.info("Primeras filas del dataset extraído:\n%s", df.head().to_string())
        return df
    except Exception as e:
        logging.error("Error al extraer datos desde %s: %s", file_path, e)
        raise
