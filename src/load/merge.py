# src/load/merge.py

import logging
import pandas as pd
import dask.dataframe as dd
import psutil
from io import StringIO
from typing import Union, Optional
import ast

# Importar la conexión (el engine) desde la base de datos definida en db.py
from src.database.db import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def log_memory_usage():
    """Registra el uso actual de memoria."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logging.info(f"Memory usage: {mem_info.rss / 1024 ** 2:.2f} MB")

def optimize_dataframe(df: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
    """Optimiza los tipos de datos para reducir el uso de memoria."""
    try:
        categorical_cols = ['track_name', 'artists', 'nominee', 'artist']
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype('category')
        if isinstance(df, pd.DataFrame):
            for col in df.select_dtypes(include=['int64']).columns:
                df[col] = pd.to_numeric(df[col], downcast='integer')
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = pd.to_numeric(df[col], downcast='float')
        return df
    except Exception as e:
        logging.error(f"Error optimizing dataframe: {str(e)}")
        return df

def normalize_text(text: str) -> str:
    """Normaliza texto para comparación exacta."""
    if not isinstance(text, str):
        return ""
    return (
        text.strip().lower()
            .replace("&", " and ")
            .replace("+", " and ")
            .translate(str.maketrans("", "", "!¡¿?.,;:-'\"()[]{}"))
    )

def merge_datasets(
    spotify_data: Union[pd.DataFrame, str],
    grammys_data: Union[pd.DataFrame, str],
    api_data: Union[pd.DataFrame, str],
    table_name: str = "merged_dataset"
) -> Optional[str]:
    """
    Une los tres datasets usando operaciones optimizadas en memoria y un manejo mejorado de la información
    de los Grammys. Solo se conservan los registros que tienen información de Grammys.

    El primer merge es entre Spotify y Grammys (inner join) utilizando como clave el track name (normalizado)
    y la columna nominee de Grammys. Posteriormente, se une el resultado con el dataset de API (left join)
    usando la columna 'artists' (de Spotify, preservada en el primer merge) y la columna 'artist' de API.

    Finalmente, se limpia el DataFrame eliminando las columnas 'categories', 'artist' y 'Unnamed: 0' antes
    de cargarlo en una tabla de la base de datos en la nube.
    """
    try:
        # 1. Cargar y optimizar datasets
        logging.info("Loading and optimizing datasets")
        spotify_df = pd.read_json(StringIO(spotify_data), orient="records") if isinstance(spotify_data, str) else spotify_data
        grammys_df = pd.read_json(StringIO(grammys_data), orient="records") if isinstance(grammys_data, str) else grammys_data
        api_df = pd.read_json(StringIO(api_data), orient="records") if isinstance(api_data, str) else api_data

        # 2. Validación de dimensiones de los datasets
        logging.info("Spotify shape: %s, Grammys shape: %s, API shape: %s",
                     spotify_df.shape, grammys_df.shape, api_df.shape)

        # 3. Normalización de claves para la unión
        spotify_df['track_name_norm'] = spotify_df['track_name'].apply(normalize_text)
        spotify_df['artists_norm'] = spotify_df['artists'].apply(normalize_text)
        grammys_df['nominee_norm'] = grammys_df['nominee'].apply(normalize_text)
        # Para el merge con API usaremos las columnas originales: 'artists' (Spotify) y 'artist' (API)

        # 4. Optimización del uso de memoria
        spotify_df = optimize_dataframe(spotify_df)
        grammys_df = optimize_dataframe(grammys_df)
        api_df = optimize_dataframe(api_df)
        log_memory_usage()

        # 5. Conversión a Dask DataFrames para procesamiento paralelo
        logging.info("Converting to Dask DataFrames")
        spotify_ddf = dd.from_pandas(spotify_df, npartitions=4)
        grammys_ddf = dd.from_pandas(grammys_df, npartitions=2)
        api_ddf = dd.from_pandas(api_df, npartitions=4)
        log_memory_usage()

        # 6. Primer merge: Spotify + Grammys (INNER JOIN para conservar solo registros con información de Grammys)
        logging.info("Starting Spotify-Grammys merge (inner join)")
        merge1 = dd.merge(
            spotify_ddf,
            grammys_ddf[['nominee_norm', 'winner', 'year', 'categories']],
            left_on="track_name_norm",
            right_on="nominee_norm",
            how="inner"
        ).repartition(npartitions=4)

        # Convertir la columna de categorías de Grammys a un string unificado
        if 'categories' in merge1.columns:
            merge1['grammy_categories'] = merge1['categories'].apply(
                lambda x: ", ".join(ast.literal_eval(x)) if pd.notna(x) else None,
                meta=('categories', 'object')
            )

        logging.info("Spotify-Grammys merge completed. Sample:")
        logging.info(merge1.head(1).to_string())
        log_memory_usage()

        # 7. Segundo merge: Unir el resultado anterior con API (LEFT JOIN en 'artists' vs 'artist')
        logging.info("Starting merge with API (left join on 'artists' vs 'artist')")
        merge_final_ddf = dd.merge(
            merge1,
            api_ddf,
            left_on="artists",  # Usando la columna original 'artists' de Spotify
            right_on="artist",
            how="left"
        ).repartition(npartitions=4)

        logging.info("Merge with API completed")
        log_memory_usage()

        # 8. Calcular el resultado final en un DataFrame de Pandas
        final_df = merge_final_ddf.compute()

        # 9. Limpieza final: eliminar columnas redundantes y gestionar valores faltantes
        final_df = final_df.drop(columns=['nominee_norm', 'artists_norm'], errors='ignore')
        final_df['won_grammy'] = final_df['winner'].fillna(False)
        final_df.drop(columns=['winner'], inplace=True, errors='ignore')
        
        # Eliminar columnas 'categories', 'artist' y 'Unnamed: 0' antes de cargar a la DB
        final_df = final_df.drop(columns=['categories', 'artist', 'Unnamed: 0'], errors='ignore')
        
        logging.info("Final merge completed with %d rows", len(final_df))
        logging.info("Final columns: %s", final_df.columns.tolist())
        log_memory_usage()

        # 10. Cargar los datos en la base de datos en la nube.
        try:
            final_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            logging.info(f"Data successfully loaded into the '{table_name}' table in the cloud DB.")
        except Exception as db_err:
            logging.error(f"Error loading data into DB: {str(db_err)}", exc_info=True)

        # Retorna el resultado final en formato JSON.
        return final_df.to_json(orient="records", date_format='iso')
        
    except Exception as e:
        logging.error("Error during merging datasets: %s", str(e), exc_info=True)
        return None

if __name__ == "__main__":
    # Ejemplo de datos de prueba
    spotify_sample = pd.DataFrame({
        "track_id": [1, 2, 3, 4],
        "artists": ["Artist A", "Artist B", "Artist C", "Billie Eilish"],
        "track_name": ["Song A", "Bad Guy", "Song C", "Bad Guy"],
        "duration_ms": [200000, 180000, 210000, 195000]
    })

    grammys_sample = pd.DataFrame({
        "nominee": ["Bad Guy", "Song X"],
        "winner": [True, False],
        "year": [2019, 2019],
        "categories": [["Record Of The Year", "Song Of The Year"], ["Best New Artist"]]
    })

    api_sample = pd.DataFrame({
        "artist": ["Artist A", "Artist C", "Billie Eilish"],
        "stat": [100, 200, 500],
        "followers": [1000, 2000, 5000],
        "ontour": [0, 0, 0],
        "listeners": [5000, 6000, 7000],
        "playcount": [10000, 20000, 30000]
    })

    merged_json = merge_datasets(
        spotify_sample.to_json(orient="records"),
        grammys_sample.to_json(orient="records"),
        api_sample.to_json(orient="records"),
        table_name="merged_dataset"
    )

    if merged_json:
        merged_df = pd.read_json(StringIO(merged_json), orient="records")
        logging.info("Merged DataFrame Sample:\n%s", merged_df.head().to_string())