import pandas as pd
from datetime import timedelta, datetime
from sqlalchemy import text
from .ingestion_raw_and_bronze import get_max_id, get_database_connection
from airflow.exceptions import AirflowSkipException

def move_data_silver_to_gold() -> None:
    engine = get_database_connection()
    ingestion_at = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    
    max_id_gold = get_max_id(engine, 'f_gold')

    query = text("""
        INSERT INTO lakehouse.f_gold (id, created_at, updated_at, ingestion_at, website, category)
        SELECT id, created_at, updated_at, :ingestion_at, website, category
        FROM lakehouse.f_silver
        WHERE id > :max_id_gold
    """)
    
    with engine.connect() as conn:
        conn.execute(query, {"ingestion_at": ingestion_at, "max_id_gold": max_id_gold})
        print(f"Dados movidos da tabela silver para gold com sucesso! Ingestão em: {ingestion_at}")

def get_new_data_for_gold(silver_table_data: str, insert_table: str, additional_query: str) -> pd.DataFrame:
    engine = get_database_connection()
    max_id_insert = get_max_id(engine, insert_table)
    
    query = text(f"""
    SELECT * FROM lakehouse.{silver_table_data}
    WHERE id > :max_id_insert AND {additional_query}
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"max_id_insert": max_id_insert})
        print(f"Número de registros extraídos: {len(df)}")
        
        if df.empty:
            raise AirflowSkipException(f"Nenhum dado novo para inserir na tabela {insert_table}.")
    
    return df

def insert_data_into_gold_notebook(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    gold_notebook = 'd_gold_notebooks'
    
    if df.empty:
        raise AirflowSkipException(f"Nenhum dado novo para inserir na tabela {gold_notebook}.")
    
    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{gold_notebook} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, CPU, GPU, RAM, SSD, specifics, cpu_category
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :CPU, :GPU, :RAM, :SSD, :specifics, :cpu_category
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
                'CPU': row['cpu'],
                'GPU': row['gpu'],
                'RAM': row['ram'],
                'SSD': row['ssd'],
                'specifics': row['specifics'],
                'cpu_category': row['cpu_category']
            }
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {gold_notebook} com sucesso.")
    
def insert_data_into_gold_tv(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    gold_tv = 'd_gold_tv'

    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{gold_tv} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, size, resolution, technology, specifics
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :size, :resolution, :technology, :specifics
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
                'technology': row['technology'],
                'specifics': row['specifics']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {gold_tv} com sucesso.")
    
def insert_data_into_gold_smartphone(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    gold_smartphone = 'd_gold_smartphone'

    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{gold_smartphone} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, RAM, storage_capacity, specifics
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :RAM, :storage_capacity, :specifics
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
                'RAM': row['ram'],
                'storage_capacity': row['storage_capacity'],
                'specifics': row['specifics']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {gold_smartphone} com sucesso.")
    
def insert_data_into_silver_tablets(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    gold_tablets = 'd_gold_tablets'


    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{gold_tablets} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, RAM, storage_capacity, specifics
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :RAM, :storage_capacity, :specifics
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
                'RAM': row['ram'],
                'storage_capacity': row['storage_capacity'],
                'specifics': row['specifics']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {gold_tablets} com sucesso.")
    
def insert_data_into_gold_smartwatch(df: pd.DataFrame) -> None:
    engine = get_database_connection()
    gold_smartwach = 'd_gold_smartwatch'
    
    with engine.connect() as conn:
        for _, row in df.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{gold_smartwach} (
                id, title, discount_price, original_price, brand, rating, link, free_freight,
                model, specifics
            ) VALUES (
                :id, :title, :discount_price, :original_price, :brand, :rating, :link, :free_freight,
                :model, :specifics
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
                'specifics': row['specifics']
            } 
            conn.execute(query, params)
    print(f"Dados novos inseridos na tabela {gold_smartwach} com sucesso.")
    
    
def execute_query_to_dataframe(query: str)-> pd.DataFrame:
    engine = get_database_connection()
    
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df