import logging
import pandas as pd
import dask.dataframe as dd
import psutil
from io import StringIO
from typing import Union, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def log_memory_usage():
    """Registra el uso actual de memoria"""
    process = psutil.Process()
    mem_info = process.memory_info()
    logging.info(f"Memory usage: {mem_info.rss / 1024 ** 2:.2f} MB")

def optimize_dataframe(df: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
    """Optimiza los tipos de datos para reducir el uso de memoria"""
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
    """Normaliza texto para comparación exacta"""
    return (
        text.strip().lower()
        .replace("&", "and")
        .translate(str.maketrans("", "", "!¡¿?.,;:-'\"+"))
    )

def merge_datasets(
    spotify_data: Union[pd.DataFrame, str],
    grammys_data: Union[pd.DataFrame, str],
    api_data: Union[pd.DataFrame, str]
) -> Optional[str]:
    """
    Merges the three datasets using memory-optimized operations
    """
    try:
        logging.info("Loading and optimizing datasets")
        spotify_df = pd.read_json(StringIO(spotify_data), orient="records") if isinstance(spotify_data, str) else spotify_data
        grammys_df = pd.read_json(StringIO(grammys_data), orient="records") if isinstance(grammys_data, str) else grammys_data
        api_df = pd.read_json(StringIO(api_data), orient="records") if isinstance(api_data, str) else api_data

        spotify_df = optimize_dataframe(spotify_df)
        grammys_df = optimize_dataframe(grammys_df)
        api_df = optimize_dataframe(api_df)
        log_memory_usage()

        logging.info("Converting to Dask DataFrames")
        spotify_ddf = dd.from_pandas(spotify_df, npartitions=4)
        grammys_ddf = dd.from_pandas(grammys_df, npartitions=2)
        api_ddf = dd.from_pandas(api_df, npartitions=4)
        log_memory_usage()

        logging.info("Starting merge operations")
        
        merge1 = dd.merge(
            spotify_ddf,
            grammys_ddf[['nominee', 'winner', 'year']], 
            left_on="track_name",
            right_on="nominee",
            how="left"
        ).repartition(npartitions=4)
        logging.info("Merge 1 completed")
        log_memory_usage()

        merge2 = dd.merge(
            spotify_ddf[['track_id', 'artists']],  
            api_ddf,
            left_on="artists",
            right_on="artist",
            how="left"
        ).repartition(npartitions=4)
        logging.info("Merge 2 completed")
        log_memory_usage()

        logging.info("Performing final merge")
        
        merge1_reduced = merge1[['track_id', 'winner', 'year']].compute()
        merge2_reduced = merge2.compute()
        
        final_df = pd.merge(
            merge1_reduced,
            merge2_reduced,
            on="track_id",
            how="outer",
            suffixes=("_grammys", "_api")
        )
        
        logging.info("Final merge completed with %d rows", len(final_df))
        log_memory_usage()

        return final_df.to_json(orient="records")
        
    except Exception as e:
        logging.error("Error during merging datasets: %s", str(e), exc_info=True)
        return None

if __name__ == "__main__":
    spotify_sample = pd.DataFrame({
        "track_id": pd.Series([1, 2, 3], dtype='int32'),
        "artists": pd.Series(["Artist A", "Artist B", "Artist C"], dtype='category'),
        "track_name": pd.Series(["Song A", "Song B", "Song C"], dtype='category'),
        "duration_ms": pd.Series([200000, 180000, 210000], dtype='int32')
    })
    
    grammys_sample = pd.DataFrame({
        "nominee": pd.Series(["Song A", "Song X"], dtype='category'),
        "winner": pd.Series([True, False], dtype='bool'),
        "year": pd.Series([2019, 2019], dtype='int16')
    })
    
    api_sample = pd.DataFrame({
        "artist": pd.Series(["Artist A", "Artist C", "Artist Z"], dtype='category'),
        "stat": pd.Series([100, 200, 300], dtype='int16')
    })
    
    merged_json = merge_datasets(
        spotify_sample.to_json(orient="records"),
        grammys_sample.to_json(orient="records"),
        api_sample.to_json(orient="records")
    )
    
    if merged_json:
        merged_df = pd.read_json(StringIO(merged_json), orient="records")
        logging.info("Merged DataFrame Sample:\n%s", merged_df.head().to_string())