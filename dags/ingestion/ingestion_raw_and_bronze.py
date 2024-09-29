from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)


def get_database_connection() -> Engine:
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')

    if not all([user, password, host, port, database]):
        raise ValueError("Ambiente inválido: todas as variáveis DB_USER, DB_PASSWORD, DB_HOST, DB_PORT e DB_NAME devem ser definidas.")

    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)
    return engine  # type: ignore

def ingest_data_to_postgres(df: pd.DataFrame, table_name: str) -> None:
    try:
        engine = get_database_connection()

        df.to_sql(table_name, engine, schema='lakehouse', if_exists='append', index=False)
        logger.info(f"Dados inseridos na tabela {table_name} com sucesso.")
    except Exception as e:
        logger.error(f"Ocorreu um erro ao inserir os dados: {e}")
        raise       
        
def get_existing_data(engine: Engine) -> pd.DataFrame:
    query = text("SELECT title, website, id, created_at FROM lakehouse.raw GROUP BY title, website, id, created_at")
    with engine.connect() as conn:
        existing_data = pd.read_sql(query, conn)
    return existing_data

def update_existing_data(df: pd.DataFrame, existing_data: pd.DataFrame) -> pd.DataFrame:
    for index, row in df.iterrows():
        match = existing_data[(existing_data['title'] == row['title']) & 
                              (existing_data['website'] == row['website'])]
        if not match.empty:
            df.at[index, 'created_at'] = match.iloc[0]['created_at']
    
    return df

def get_max_id(engine: Engine, table_name: str) -> int:
    query = text(f"SELECT MAX(id) as max_id FROM lakehouse.{table_name}")
    
    with engine.connect() as conn:
        result = conn.execute(query)
        
        if result:  
            row = result.fetchone()
            if row and row[0] is not None: 
                return row[0]
    
    return 0

def insert_into_fact_bronze(engine: Engine):
    max_id = get_max_id(engine, "f_bronze")
    
    query = text("""
        INSERT INTO lakehouse.f_bronze (id, created_at, updated_at, website, category)
        SELECT id, created_at, updated_at, website, category
        FROM lakehouse.raw
        WHERE id > :max_id
    """)
    
    with engine.connect() as conn:
        conn.execute(query, {'max_id': max_id})

def insert_into_dimension(engine: Engine, dimension_table: str, categories: list) -> None:
    max_id = get_max_id(engine, dimension_table)
    categories_placeholder = ','.join([f":category_{i}" for i in range(len(categories))])
    
    query = text(f"""
        INSERT INTO lakehouse.{dimension_table} (id, title, discount_price, original_price, brand, rating, link, free_freight)
        SELECT id, title, discount_price, original_price, brand, rating, link, free_freight
        FROM lakehouse.raw
        WHERE id > :max_id AND category IN ({categories_placeholder})
    """)

    params = {'max_id': max_id}
    for i, category in enumerate(categories):
        params[f'category_{i}'] = category
    
    with engine.connect() as conn:
        conn.execute(query, params)