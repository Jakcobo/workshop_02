import json
import logging
from typing import Union, Optional, Dict, List
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def transform_spotify_data(df: Union[pd.DataFrame, str]) -> Optional[str]:
    """
    Cleans and transforms the Spotify DataFrame.
    
    This function performs several data cleaning and transformation operations on the Spotify dataset,
    including removing duplicates, filtering out rows where the 'artists', 'album_name', or 'track_name'
    columns are null, and remapping the 'track_genre' column to a new 'genre_category' column using
    a custom mapping that reorganizes genre groups into more distinct categories.
    
    Args:
        df (Union[pd.DataFrame, str]): Input DataFrame or JSON string.
        
    Returns:
        Optional[str]: Transformed DataFrame as a JSON string, or None if an error occurs.
        
    Raises:
        ValueError: If the input DataFrame is empty or required columns are missing.
    """
    try:
        if isinstance(df, str):
            try:
                df = pd.DataFrame(json.loads(df))
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON string provided: {str(e)}")
                return None
        
        if df.empty:
            raise ValueError("Input DataFrame is empty")
        
        logging.info(f"Starting Spotify data transformation. Original shape: {df.shape}")

        if "track_id" in df.columns:
            df = df.drop_duplicates(subset=["track_id"]).reset_index(drop=True)

        required_columns = ["artists", "album_name", "track_name"]
        initial_rows = len(df)
        df = df.dropna(subset=required_columns).reset_index(drop=True)
        logging.info("Removed %d rows with null in %s.", initial_rows - len(df), required_columns)
    
        df = df.drop_duplicates().reset_index(drop=True)

        genre_mapping: Dict[str, List[str]] = {
            "Rock": [
                "alt-rock", "alternative", "grunge", "indie", "punk-rock", "rock-n-roll", "rock"
            ],
            "Metal": [
                "black-metal", "death-metal", "heavy-metal", "metal", "metalcore", "industrial", "hardcore", "grindcore"
            ],
            "Pop": [
                "pop", "indie-pop", "power-pop", "k-pop", "j-pop", "mandopop", "cantopop", "synth-pop", "pop-film"
            ],
            "Electronic": [
                "edm", "electro", "electronic", "house", "deep-house", "progressive-house",
                "techno", "trance", "dubstep", "drum-and-bass", "dub", "garage", "idm",
                "club", "dance", "minimal-techno", "detroit-techno", "chicago-house",
                "breakbeat", "hardstyle", "trip-hop"
            ],
            "Hip-Hop_&_R&B": [
                "hip-hop", "r-n-b", "rap", "dancehall"
            ],
            "Latin&Reggaeton": [
                "brazil", "salsa", "samba", "spanish", "pagode", "sertanejo",
                "mpb", "latin", "latino", "reggaeton", "reggae"
            ],
            "World": [
                "indian", "iranian", "malay", "turkish", "tango", "afrobeat",
                "french", "german", "british", "swedish"
            ],
            "Jazz&Soul": [
                "blues", "bluegrass", "funk", "gospel", "jazz", "soul", "groove", "disco", "ska"
            ],
            "Classical&Instrumental": [
                "acoustic", "classical", "guitar", "piano", "opera", "new-age", "world-music"
            ],
            "Mood": [
                "ambient", "chill", "happy", "sad", "sleep", "study"
            ],
            "Varied": [
                "children", "disney", "forro", "kids", "party", "romance", "show-tunes",
                "comedy", "anime", "honky-tonk", "folk", "singer-songwriter"
            ]
        }

        def map_genre(genre: str) -> str:
            genre_lower = genre.lower() if isinstance(genre, str) else ""
            for category, keywords in genre_mapping.items():
                for keyword in keywords:
                    if keyword in genre_lower:
                        return category
            return "Varied/Other"

        if "track_genre" not in df.columns:
            logging.error("Missing 'track_genre' column in DataFrame")
            raise ValueError("Missing 'track_genre' column")

        df["genre_category"] = df["track_genre"].apply(map_genre)
        logging.info("Mapped 'track_genre' to 'genre_category' successfully.")
        
        logging.info(f"Transformation complete. Final shape: {df.shape}")
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during Spotify data transformation: {str(e)}")
        return None


if __name__ == "__main__":
    sample_data = {
        "Unnamed: 0": [0, 1, 2, 3],
        "track_id": [
            "5SuOikwiRyPMVoIQDJUgSV", "4qPNDBW1i3p13qLCt0Ki3A",
            "1iJBSr7s7jYXzM8EGcbK5b", "6lfxq3CG4xtTiEg7opyCyx"
        ],
        "artists": ["Gen Hoshino", "Ben Woodward", "Ingrid Michaelson;ZAYN", "Kina Grannis"],
        "album_name": [
            "Comedy", "Ghost (Acoustic)", "To Begin Again",
            "Crazy Rich Asians (Original Motion Picture Sou...)"
        ],
        "track_name": ["Comedy", "Ghost - Acoustic", "To Begin Again", "Can't Help Falling In Love"],
        "popularity": [73, 55, 57, 71],
        "duration_ms": [230666, 149610, 210826, 201933],
        "explicit": [False, False, False, False],
        "danceability": [0.676, 0.420, 0.438, 0.266],
        "energy": [0.461, 0.166, 0.359, 0.06],
        "loudness": [-6.746, -17.235, -9.734, -18.515],
        "mode": [0, 1, 1, 1],
        "speechiness": [0.143, 0.076, 0.0557, 0.0363],
        "acousticness": [0.0322, 0.924, 0.21, 0.905],
        "instrumentalness": [0.000001, 0.000006, 0.0, 0.000071],
        "liveness": [0.358, 0.101, 0.117, 0.132],
        "valence": [0.715, 0.267, 0.12, 0.143],
        "tempo": [87.917, 77.489, 76.332, 181.74],
        "time_signature": [4, 4, 4, 3],
        "track_genre": ["acoustic", "acoustic", "acoustic", "acoustic"]
    }
    df_sample = pd.DataFrame(sample_data)
    logging.info("Original Spotify DataFrame:\n%s", df_sample.to_string())
    transformed_json = transform_spotify_data(df_sample)
    if transformed_json:
        transformed_df = pd.read_json(transformed_json, orient="records")
        logging.info("Transformed Spotify DataFrame:\n%s", transformed_df.to_string())