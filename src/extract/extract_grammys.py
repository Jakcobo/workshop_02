# src/extract/extract_grammys.py

import pandas as pd
import logging
from sqlalchemy import text
from src.database.db import get_session

def extract_grammys_data():
    """
    Extrae los datos de la tabla 'grammys' desde la base de datos PostgreSQL.
    Retorna un DataFrame de pandas con los datos extraídos.
    """
    session = get_session()
    try:
        # Se define la consulta para extraer todos los datos de la tabla 'grammys'
        query = text("SELECT * FROM grammys")
        result = session.execute(query)
        rows = result.fetchall()
        columns = result.keys()
        df = pd.DataFrame(rows, columns=columns)
        logging.info("Datos extraídos correctamente de la tabla 'grammys'.")
        return df
    except Exception as e:
        logging.error(f"Error extrayendo datos de la tabla grammys: {e}")
        raise
    finally:
        session.close()
