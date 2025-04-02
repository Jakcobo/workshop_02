import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

project_root = os.path.abspath(os.path.dirname(__file__) + "/..")

if project_root not in sys.path:    
    sys.path.append(project_root)


dotenv_path = os.path.join(project_root, "env/.env")
load_dotenv(dotenv_path, override=True)

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")

def create_engine_connection():
    engine = create_engine(f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")

    return engine
