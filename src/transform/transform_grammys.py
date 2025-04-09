import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def transform_grammys_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the Grammys DataFrame by selecting only the required columns
    ('category', 'nominee', 'winner', 'year') and removing rows where 'nominee' is null.
    
    Parameters:
        df (pd.DataFrame): The original Grammys DataFrame.
        
    Returns:
        pd.DataFrame: The transformed DataFrame containing only the specified columns
                      with all rows having a non-null 'nominee'.
    """
    required_columns = ['category', 'nominee', 'winner', 'year']
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logging.error("Missing required columns: %s", missing_columns)
        raise ValueError(f"Missing columns: {missing_columns}")
    
    transformed_df = df[required_columns]
    transformed_df = transformed_df.dropna(subset=['nominee'])
    
    logging.info("Transformation complete: resulting DataFrame has %d rows.", len(transformed_df))
    return transformed_df

if __name__ == "__main__":
  
    data = {
        "year": [2019, 2019, 2019],
        "title": [
            "62nd Annual GRAMMY Awards (2019)",
            "62nd Annual GRAMMY Awards (2019)",
            "62nd Annual GRAMMY Awards (2019)"
        ],
        "published_at": [
            "2020-05-19T05:10:28-07:00",
            "2020-05-19T05:10:28-07:00",
            "2020-05-19T05:10:28-07:00"
        ],
        "updated_at": [
            "2020-05-19T05:10:28-07:00",
            "2020-05-19T05:10:28-07:00",
            "2020-05-19T05:10:28-07:00"
        ],
        "category": ["Record Of The Year", "Record Of The Year", "Record Of The Year"],
        "nominee": ["Bad Guy", None, "7 rings"],
        "artist": ["Billie Eilish", "Bon Iver", "Ariana Grande"],
        "workers": [
            "Finneas O'Connell, producer; Rob Kinelski & Fi...",
            "BJ Burton, Brad Cook, Chris Messina & Justin V...",
            "Charles Anderson, Tommy Brown, Michael Foster ..."
        ],
        "img": [
            "https://www.grammy.com/sites/com/files/styles/...",
            "https://www.grammy.com/sites/com/files/styles/...",
            "https://www.grammy.com/sites/com/files/styles/..."
        ],
        "winner": [True, True, True]
    }
    
    df_sample = pd.DataFrame(data)
    logging.info("Original DataFrame:\n%s", df_sample.to_string())
    
    df_transformed = transform_grammys_data(df_sample)
    logging.info("Transformed DataFrame:\n%s", df_transformed.to_string())
