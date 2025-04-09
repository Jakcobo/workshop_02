import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

project_root = os.path.abspath(os.path.dirname(__file__) + "/../..")

if project_root not in sys.path:    
    sys.path.append(project_root)


dotenv_path = os.path.join(project_root, "env/.env")
load_dotenv(dotenv_path, override=True)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("The DATABASE_URL variable was not found in the .env file")

def create_engine_connection():
    engine = create_engine(DATABASE_URL)
    return engine