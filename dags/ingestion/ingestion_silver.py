import sqlalchemy as sa
from datetime import datetime, timedelta
from sqlalchemy import text
import spacy
import pandas as pd
from .ingestion_raw_and_bronze import get_database_connection, get_max_id
from utils.utils import normalize_storage, notebook_normalization

def get_data_from_dimension(dimension_table):
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

def load_ner_model(model_path):
    return spacy.load(model_path)


import pandas as pd

def apply_ner_to_title(df, nlp):
    def entities_to_dataframe(text, doc):
        entities = {
            'model': '',
            'CPU': '',
            'GPU': '',
            'RAM': '',
            'SSD': ''
        }
        for ent in doc.ents:
            if ent.label_ == 'MODEL':
                entities['model'] = ent.text
            elif ent.label_ == 'CPU':
                entities['CPU'] = ent.text
            elif ent.label_ == 'GPU':
                entities['GPU'] = ent.text
            elif ent.label_ == 'RAM':
                entities['RAM'] = ent.text
            elif ent.label_ == 'SSD':
                entities['SSD'] = ent.text
        return entities
    
    def remove_invalid_ssd(df):
        return df[df['SSD'].apply(lambda x: len(str(x)) <= 6)]
    def remove_invalid_ram(df):
        return df[df['RAM'].apply(lambda x: len(str(x)) <= 5)]
    
    new_entities = []
    for title in df['title']:
        doc = nlp(title)
        entities = entities_to_dataframe(title, doc)
        new_entities.append(entities)

    df_entities = pd.DataFrame(new_entities)
    df = pd.concat([df, df_entities], axis=1)
    df['RAM'] = df['RAM'].apply(normalize_storage)
    df['SSD'] = df['SSD'].apply(normalize_storage)
    
    df = remove_invalid_ssd(df)
    df = remove_invalid_ram(df)
    df = df[df.apply(notebook_normalization, axis=1) != 3]
    
    return df


def insert_data_into_silver_notebook(df, silver_table):
    engine = get_database_connection()
    max_id = get_max_id(engine, silver_table)
    
    df_filtered = df[df['id'] > max_id]

    if df_filtered.empty:
        print(f"Nenhum dado novo para inserir na tabela {silver_table}.")
        return
    
    with engine.connect() as conn:
        for _, row in df_filtered.iterrows():
            query = text(f"""
            INSERT INTO lakehouse.{silver_table} (
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
    print(f"Dados novos inseridos na tabela {silver_table} com sucesso.")