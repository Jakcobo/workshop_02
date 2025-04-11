# src/load/store_drive.py

import os
import logging
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

# Load environment variables
env_path = Path(__file__).parent.parent.parent / "../../env/.env"
load_dotenv(dotenv_path=env_path)

config_dir = Path(os.getenv("CONFIG_DIR")).resolve()
client_secrets_file = config_dir / "client_secrets.json"
settings_file = config_dir / "settings.yaml"
credentials_file = config_dir / "saved_credentials.json"
folder_id = os.getenv("FOLDER_ID")

def auth_drive():
    """
    Authenticates and returns a Google Drive instance using PyDrive2.
    """
    try:
        logging.info("Starting Google Drive authentication process.")
        gauth = GoogleAuth(settings_file=str(settings_file))
        
        if credentials_file.exists():
            gauth.LoadCredentialsFile(str(credentials_file))
            if gauth.access_token_expired:
                logging.info("Access token expired, refreshing token.")
                gauth.Refresh()
            else:
                logging.info("Using saved credentials.")
        else:
            logging.info("Saved credentials not found, performing web authentication.")
            gauth.LoadClientConfigFile(str(client_secrets_file))
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(str(credentials_file))
            logging.info("Credentials saved successfully.")

        drive = GoogleDrive(gauth)
        logging.info("Google Drive authentication completed.")
        return drive

    except Exception as e:
        logging.error(f"Authentication error: {e}", exc_info=True)
        raise

def storing_merged_data(title: str, df: pd.DataFrame):
    """
    Uploads the given DataFrame as a CSV file to Google Drive.
    
    :param title: The name of the file to be stored.
    :param df: The pandas DataFrame to upload.
    """
    try:
        drive = auth_drive()
        csv_file = df.to_csv(index=False)
        
        file = drive.CreateFile({
            "title": title,
            "parents": [{"kind": "drive#fileLink", "id": folder_id}],
            "mimeType": "text/csv"
        })
        file.SetContentString(csv_file)
        file.Upload()
        
        logging.info(f"File '{title}' uploaded successfully to Google Drive.")
    except Exception as e:
        logging.error(f"Failed to upload file to Google Drive: {e}", exc_info=True)
