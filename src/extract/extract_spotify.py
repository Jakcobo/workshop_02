# src/extract/extract_spotify.py

import os
import pandas as pd

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
        print(f"Datos extraídos correctamente desde {file_path}.")
        return df
    except Exception as e:
        print(f"Error al extraer datos desde {file_path}: {e}")
        raise
