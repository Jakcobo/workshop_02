import os
import pandas as pd
import logging

def extract_spotify_data():
    """
    Extrae los datos del dataset de Spotify desde un archivo CSV ubicado en 'data/dataset_spotify.csv'.
    Retorna un DataFrame de pandas.
    """
    # Construir la ruta absoluta al archivo CSV; se asume que 'data' está en la raíz del proyecto.
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    file_path = os.path.join(base_dir, 'data', 'dataset_spotify.csv')
    
    try:
        df = pd.read_csv(file_path)
        logging.info("Datos extraídos correctamente desde %s.", file_path)
        return df
    except Exception as e:
        logging.error("Error al extraer datos desde %s: %s", file_path, e)
        raise
