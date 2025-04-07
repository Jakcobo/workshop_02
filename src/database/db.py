# src/database/db.py

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import database_exists, create_database

# Cargar variables de entorno desde .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../env/.env'))

# Leer credenciales de PostgreSQL desde las variables de entorno
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT") or "5432"
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")

# Construir la URL de conexión
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Crear el engine de SQLAlchemy y verificar si la base de datos existe
try:
    engine = create_engine(DATABASE_URL, echo=False)
    if not database_exists(engine.url):
        create_database(engine.url)
        logging.info("Base de datos creada exitosamente.")
except Exception as e:
    logging.error(f"Error al crear el engine o la base de datos: {e}")
    raise

# Configurar la sesión
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Declarative base para definir modelos
Base = declarative_base()

# Modelo de ejemplo: tabla 'ejemplo'
class Ejemplo(Base):
    __tablename__ = "ejemplo"
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String, nullable=False)
    valor = Column(Text)

def init_db():
    """
    Inicializa la base de datos creando todas las tablas definidas en los modelos.
    """
    try:
        Base.metadata.create_all(bind=engine)
        logging.info("Base de datos inicializada correctamente.")
    except SQLAlchemyError as e:
        logging.error(f"Error al inicializar la base de datos: {e}")
        raise

def get_session():
    """
    Crea y retorna una sesión para interactuar con la base de datos.
    """
    try:
        session = SessionLocal()
        return session
    except SQLAlchemyError as e:
        logging.error(f"Error al crear la sesión: {e}")
        raise

def insert_ejemplo(data: dict):
    """
    Inserta un registro en la tabla 'ejemplo'.
    
    :param data: Diccionario con los datos a insertar. Se espera que tenga las claves 'nombre' y 'valor'.
    """
    session = get_session()
    try:
        ejemplo = Ejemplo(**data)
        session.add(ejemplo)
        session.commit()
        logging.info("Registro insertado correctamente en 'ejemplo'.")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Error al insertar datos en 'ejemplo': {e}")
        raise
    finally:
        session.close()

def fetch_ejemplos():
    """
    Recupera todos los registros de la tabla 'ejemplo'.
    
    :return: Lista de objetos de tipo Ejemplo.
    """
    session = get_session()
    try:
        registros = session.query(Ejemplo).all()
        logging.info("Datos recuperados correctamente de 'ejemplo'.")
        return registros
    except SQLAlchemyError as e:
        logging.error(f"Error al recuperar datos de 'ejemplo': {e}")
        raise
    finally:
        session.close()
