from datetime import datetime, timedelta
from sqlalchemy import text
import pandas as pd
from .ingestion_raw_and_bronze import get_database_connection, get_max_id

def get_data_from_dimension(dimension_table: str) -> pd.DataFrame:
    engine = get_database_connection()
    query = f"""
    SELECT id, title, discount_price, original_price, brand, rating, link, free_freight
    FROM lakehouse.{dimension_table}
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(f"Número de registros extraídos: {len(df)}")
    return df

def filter_for_max_id(df: pd.DataFrame , insert_table: str) -> pd.DataFrame | None:
    engine = get_database_connection()
    max_id = get_max_id(engine, insert_table)
    
    df_filtered = df[df['id'] > max_id]
    
    if df_filtered.empty:
        print(f"Nenhum dado novo para inserir na tabela {insert_table}.")
        raise
    
    df = df_filtered
    return df

def move_data_bronze_to_silver() -> None:
    engine = get_database_connection()
    ingestion_at = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    
    max_id_silver = get_max_id(engine, 'f_silver')

    query = text("""
        INSERT INTO lakehouse.f_silver (id, created_at, updated_at, ingestion_at, website, category)
        SELECT id, created_at, updated_at, :ingestion_at, website, category
        FROM lakehouse.f_bronze
        WHERE id > :max_id_silver
    """)
    
    with engine.connect() as conn:
        conn.execute(query, {"ingestion_at": ingestion_at, "max_id_silver": max_id_silver})
        print(f"Dados movidos da tabela bronze para silver com sucesso! Ingestão em: {ingestion_at}")


def insert_data_into_silver_notebook(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    silver_notebook = 'd_silver_notebooks'
    
    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{silver_notebook} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, CPU, GPU, RAM, SSD
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :CPU, :GPU, :RAM, :SSD
            )
            ON CONFLICT (id) DO NOTHING
            """)

            params = {
                'id': row['id'],
                'title': row['title'],
                'discount_price': row['discount_price'],
                'original_price': row['original_price'],
                'brand': row['brand'],
                'rating': row['rating'],
                'link': row['link'],
                'free_freight': row['free_freight'],
                'model': row['model'],
                'CPU': row['CPU'],
                'GPU': row['GPU'],
                'RAM': row['RAM'],
                'SSD': row['SSD']
            }
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {silver_notebook} com sucesso.")
    
def insert_data_into_silver_tv(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    silver_tv = 'd_silver_tv'

    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{silver_tv} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, size, resolution, technology
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :size, :resolution, :technology
            )
            ON CONFLICT (id) DO NOTHING
            """)

            params = {
                'id': row['id'],
                'title': row['title'],
                'discount_price': row['discount_price'],
                'original_price': row['original_price'],
                'brand': row['brand'],
                'rating': row['rating'],
                'link': row['link'],
                'free_freight': row['free_freight'],
                'model': row['model'],
                'size': row['size'],
                'resolution': row['resolution'],
                'technology': row['technology']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {silver_tv} com sucesso.")


def insert_data_into_silver_smartwatch(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    silver_smartwach = 'd_silver_smartwatch'
    
    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{silver_smartwach} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model
            )
            ON CONFLICT (id) DO NOTHING 
            """)

            params = {
                'id': row['id'],
                'title': row['title'],
                'discount_price': row['discount_price'],
                'original_price': row['original_price'],
                'brand': row['brand'],
                'rating': row['rating'],
                'link': row['link'],
                'free_freight': row['free_freight'],
                'model': row['model']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {silver_smartwach} com sucesso.")
    
    
def insert_data_into_silver_tablets(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    silver_tablets = 'd_silver_tablets'


    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{silver_tablets} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, RAM, storage_capacity
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :RAM, :storage_capacity
            )
            ON CONFLICT (id) DO NOTHING 
            """)

            params = {
                'id': row['id'],
                'title': row['title'],
                'discount_price': row['discount_price'],
                'original_price': row['original_price'],
                'brand': row['brand'],
                'rating': row['rating'],
                'link': row['link'],
                'free_freight': row['free_freight'],
                'model': row['model'],
                'RAM': row['RAM'],
                'storage_capacity': row['storage_capacity']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {silver_tablets} com sucesso.")
    
def insert_data_into_silver_smartphone(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    silver_smartphone = 'd_silver_smartphone'

    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{silver_smartphone} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, RAM, storage_capacity
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :RAM, :storage_capacity
            )
            ON CONFLICT (id) DO NOTHING 
            """)

            params = {
                'id': row['id'],
                'title': row['title'],
                'discount_price': row['discount_price'],
                'original_price': row['original_price'],
                'brand': row['brand'],
                'rating': row['rating'],
                'link': row['link'],
                'free_freight': row['free_freight'],
                'model': row['model'],
                'RAM': row['RAM'],
                'storage_capacity': row['storage_capacity']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {silver_smartphone} com sucesso.")