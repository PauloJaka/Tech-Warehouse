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

def get_new_data_for_gold(silver_table_data: str, insert_table:str) -> pd.DataFrame | None:
    engine = get_database_connection()
    max_id_insert = get_max_id(engine, insert_table)
    
    query = f"""
    SELECT * FROM lakehouse.{silver_table_data}
    WHERE id > {max_id_insert} 
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn) 
        print(f"Número de registros extraídos: {len(df)}")
        
        if df.empty:
            raise AirflowSkipException(f"Nenhum dado novo para inserir na tabela {insert_table}.")
    return df