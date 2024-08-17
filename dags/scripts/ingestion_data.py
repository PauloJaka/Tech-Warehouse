from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_database_connection():
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')

    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)
    return engine

def ingest_data_to_postgres(df, table_name):
    try:
        engine = get_database_connection()

        df.to_sql(table_name, engine, schema='lakehouse', if_exists='append', index=False)
        logger.info(f"Dados inseridos na tabela {table_name} com sucesso.")
    except Exception as e:
        logger.error(f"Ocorreu um erro ao inserir os dados: {e}")