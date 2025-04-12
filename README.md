# Workshop 02

### Spotify - Grammy - LAST.fm

# Workshop 02 – ETL Pipeline for Music Data

This repository implements an end-to-end ETL (Extract, Transform, Load) pipeline designed to process and merge multiple music-related datasets. The pipeline extracts data from three sources—a CSV file containing Spotify tracks, a PostgreSQL database storing GRAMMY Award data, and a CSV file (from an API) for LastFM artist statistics. After extracting and transforming the data, the pipeline merges the datasets into a single final table and uploads the resulting CSV file to Google Drive using PyDrive2.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Setup and Configuration](#setup-and-configuration)
- [Running the Pipeline](#running-the-pipeline)
- [ETL Pipeline Details](#etl-pipeline-details)
- [Google Drive Authentication](#google-drive-authentication)
- [Troubleshooting](#troubleshooting)
- [License](#license)
---

## Project Overview

This project demonstrates the creation of a robust ETL pipeline using Apache Airflow. The main objectives are to:

- **Extract:**
    - Read Spotify track data from a CSV file.
    - Retrieve GRAMMY Award data from a PostgreSQL database.
    - Import LastFM artist statistics from an API CSV file.
- **Transform:**
    - Clean and standardize each dataset (remove duplicates and null values, normalize keys, and adjust genre classifications).
    - For GRAMMY data, filter only winners, remove null nominee records, and group multiple categories for the same song.
- **Merge:**
    - Join the transformed datasets into one final table based on defined keys (e.g., matching Spotify `track_name` with GRAMMY `nominee`, and Spotify `artists` with API `artist`).
- **Load:**
    - Upload the final merged dataset (as a CSV file) to a specified folder on Google Drive.

This solution is ideal for those seeking to build scalable, reproducible ETL workflows for music analytics.

---

## Repository Structure

```
workshop_02/
├── airflow/
│   ├── dags/                   # Airflow DAG definitions
│   │   └── dags.py
│   └── task/                   # Python task functions used by the DAGs
│       ├── __init__.py
│       └── task.py
├── notebooks/                  # EDAs for each dataset 
|   ├── 001_EDA_spotify.ipynb
│   ├── 002_EDA_grammys.ipynb
│   └── 003_EDA_api.ipynb
├── data/                       # Data sources
│   ├── dataset_spotify.csv     # Spotify tracks dataset
│   ├── lastfm_artist_stats.csv # LastFM artist statistics dataset
│   └── the_grammy_awards.csv   # Grammys dataset for coneccion.pý
├── env/                        # Environment variables file
│   └── .env
└── src/                        # Source code for ETL
    ├── database/               # Database connections and operations
    │   ├── db.py 
    │   └──conecction.py        # This file is for coneccion to db for EDA
    ├── extract/                # Extraction modules for each data source
    │   ├── extract_spotify.py
    │   ├── extract_grammys.py
    │   └── extract_api.py
    ├── transform/              # Transformation modules for data cleaning
    │   ├── transform_spotify.py
    │   └── transform_grammys.py
    └── load/                   # Loading modules (merge and Google Drive upload)
        ├── merge.py
        └── drive.py
```

---

## Prerequisites

- **Python 3.9+**
- **Apache Airflow** (recommended via official Docker image or virtual environment)
- **PostgreSQL** (as the source for GRAMMY Award data)
- **PyDrive2** for Google Drive integration
- **psutil, dask, and pandas** for processing and optimization
- **Google Cloud account** with Drive API enabled

---

## Setup and Configuration

### 1. Clone the Repository

```bash
git clone https://github.com/Jakcobo/workshop_02.git
cd workshop_02
```

### 2. Create and Activate a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Set Up Environment Variables

Create a file named `.env` in the `env/` folder with the following content:

```
CONFIG_DIR=config
FOLDER_ID=1iQ7-FBwAqGpDY0USk2mE5oneGbbDY8au
PG_HOST=your_postgres_host
PG_PORT=your_postgres_port
PG_USER=your_postgres_user
PG_PASSWORD=your_postgres_password
PG_DATABASE=your_postgres_database
```

Replace placeholders with your actual credentials.

### 4. Configure Google API

Place your `client_secrets.json` in the `config/` folder. Then create a `settings.yaml` file in the same folder with this content:

```yaml
client_config_backend: file
client_config_file: client_secrets.json

save_credentials: True
save_credentials_backend: file
save_credentials_file: saved_credentials.json

get_refresh_token: True
oauth_scope:
  - https://www.googleapis.com/auth/drive.file
```

---

## Running the Pipeline

1. **Start Airflow:**
    
    If using Airflow standalone, run:
    
    ```bash
    airflow standalone
    ```
    
    Otherwise, ensure your scheduler and webserver are running.
    
2. **Trigger the DAG:**
    
    Open your Airflow web interface (usually at [http://localhost:8080](http://localhost:8080/)) and trigger the `etl_dag` manually.
    
3. **Monitor the Pipeline:**
    
    Check task logs through the Airflow UI to verify that the extraction, transformation, merging, and storage steps run successfully.
    

---

## ETL Pipeline Details

### Extraction

- **Spotify:** Reads track data from `dataset_spotify.csv`.
- **GRAMMY:** Retrieves award data from a PostgreSQL table.
- **API (LastFM):** Reads artist stats from `lastfm_artist_stats.csv`.

### Transformation

- **Spotify Transformation:** Cleans and standardizes data, removes duplicates and nulls, and remaps genres to a new column (`genre_category`).
- **GRAMMY Transformation:** Filters records for winners only, removes null nominees, normalizes song names, and aggregates multiple categories per song.

### Merge

- **Merge Step:**
    - **Merge 1:** Joins Spotify and GRAMMY datasets using Spotify's `track_name` and GRAMMY's `nominee`.
    - **Merge 2:** Joins Spotify and API datasets using Spotify's `artists` and API's `artist`.
    - These two merges are then combined (outer join on Spotify’s unique `track_id`) to create a single final dataset.

### Google Drive Upload

- **Storage Step:** The final merged dataset is uploaded to Google Drive as a CSV file using PyDrive2.
- **Authentication:** Handled via OAuth2 using files from the `config/` folder. On the first run, a browser window will open to authenticate with your Google account, and credentials will be saved for future use.

---

## Google Drive Authentication

- The pipeline uses PyDrive2 to authenticate and interact with Google Drive.
- **Required Config Files:**
    - `client_secrets.json` (downloaded from Google Cloud Console)
    - `settings.yaml` (defines configuration for OAuth2 and credential storage)
    - `saved_credentials.json` (generated after successful authentication)
- Environment variable settings (in `.env`) define the folder where the merged CSV will be uploaded.

---

## Troubleshooting

- **Missing or Invalid Credentials:**
    
    Ensure that the `CONFIG_DIR` is correctly set and that `client_secrets.json` and `settings.yaml` exist in that folder.
    
- **Memory Issues:**
    
    If tasks are killed due to memory overload (e.g., during merge), consider optimizing DataFrame types, processing data in chunks, or increasing allocated resources to Airflow workers.
    
- **XCom Issues:**
    
    Verify that the `task_ids` in `xcom_pull` calls in your `task.py` exactly match the ones defined in your `dags.py`.
    
- **Google API Not Enabled:**
    
    If you encounter API errors, make sure that the Google Drive API is enabled in your Google Cloud project.