from datetime import datetime, timedelta
from sqlalchemy import text
import pandas as pd
from .ingestion_raw_and_bronze import get_database_connection, get_max_id

def get_data_from_dimension(dimension_table: str):
    engine = get_database_connection()
    query = f"""
    SELECT id, title, discount_price, original_price, brand, rating, link, free_freight
    FROM lakehouse.{dimension_table}
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(f"Número de registros extraídos: {len(df)}")
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


def insert_data_into_silver_notebook(df):
    engine = get_database_connection()
    silver_notebook = 'd_silver_notebooks'
    max_id = get_max_id(engine, silver_notebook)

    
    df_filtered = df[df['id'] > max_id]

    if df_filtered.empty:
        print(f"Nenhum dado novo para inserir na tabela {silver_notebook}.")
        return
    
    with engine.connect() as conn:
        for _, row in df_filtered.iterrows():
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