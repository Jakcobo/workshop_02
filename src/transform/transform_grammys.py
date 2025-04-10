# src/transform/transform_grammys.py

import pandas as pd
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def transform_grammys_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Transforma el dataset de los Grammy para prepararlo para el merge con Spotify:
    1. Filtra solo los ganadores reales (winner=True)
    2. Elimina registros con nominee nulo
    3. Agrupa múltiples categorías por canción
    4. Normaliza los nombres de canciones

    Parameters:
        df (pd.DataFrame): DataFrame original de los Grammy

    Returns:
        pd.DataFrame: DataFrame transformado con columnas:
                      ['nominee', 'year', 'categories', 'winner']
    """
    try:
        # 1. Validar columnas requeridas
        required_columns = ['category', 'nominee', 'winner', 'year']
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logging.error("Columnas faltantes: %s", missing_columns)
            raise ValueError(f"Columnas requeridas faltantes: {missing_columns}")

        # 2. Filtrar datos
        transformed_df = df[required_columns].copy()
        
        # Eliminar registros con nominee nulo
        transformed_df = transformed_df.dropna(subset=['nominee'])
        
        # Filtrar SOLO los ganadores reales (evitar registros donde winner=False)
        transformed_df = transformed_df[transformed_df['winner'] == True]
        
        # 3. Normalizar nombres de canciones
        transformed_df['nominee_normalized'] = (
            transformed_df['nominee']
            .str.lower()
            .str.strip()
            .str.replace(r'[^\w\s]', '', regex=True)
        )

        # 4. Agrupar múltiples categorías por canción
        result_df = transformed_df.groupby(['nominee_normalized', 'year']).agg({
            'category': lambda x: list(x),
            'winner': 'first'
        }).reset_index()

        # Renombrar columnas para claridad
        result_df = result_df.rename(columns={
            'category': 'categories',
            'nominee_normalized': 'nominee'
        })

        # Seleccionar columnas finales
        result_df = result_df[['nominee', 'year', 'categories', 'winner']]

        logging.info("Transformación completada. Registros resultantes: %d", len(result_df))
        logging.debug("Ejemplo de datos transformados:\n%s", result_df.head(3).to_string())
        
        return result_df

    except Exception as e:
        logging.error("Error transformando datos de los Grammy: %s", str(e), exc_info=True)
        return None

if __name__ == "__main__":
    # Datos de prueba mejorados
    test_data = {
        "year": [2019, 2019, 2019, 2020, 2020],
        "category": [
            "Record Of The Year", 
            "Song Of The Year", 
            "Record Of The Year",
            "Best Pop Solo Performance",
            "Record Of The Year"
        ],
        "nominee": ["Bad Guy", "Bad Guy", "7 Rings", "Don't Start Now", "Everything I Wanted"],
        "winner": [True, True, False, True, True]  # Nota: 7 Rings es False
    }

    df_test = pd.DataFrame(test_data)
    logging.info("=== Datos de prueba originales ===")
    logging.info(df_test.to_string())

    transformed_data = transform_grammys_data(df_test)
    
    if transformed_data is not None:
        logging.info("\n=== Datos transformados ===")
        logging.info(transformed_data.to_string())
        logging.info("\nEjemplo de salida JSON:")
        logging.info(transformed_data.head(2).to_json(orient='records', indent=2))