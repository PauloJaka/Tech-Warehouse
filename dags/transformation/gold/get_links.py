import pandas as pd
from sqlalchemy import Table, MetaData
from sqlalchemy.exc import IntegrityError
from pytube import Search
from ingestion.ingestion_raw_and_bronze import get_database_connection

engine = get_database_connection()

def get_first_youtube_link(produto: str, model: str) -> str | None:
    query = f"Review {produto} {model}"
    search = Search(query)
    
    if search.results:
        return search.results[0].watch_url
    return None

def insert_new_models(df: pd.DataFrame) -> None:
    with engine.connect() as connection:
        existing_models = pd.read_sql("SELECT model FROM lakehouse.unique_models", connection)
    
    existing_models_list = existing_models['model'].str.lower().tolist() 

    new_models = df[~df['model'].str.lower().isin(existing_models_list)]
    
    if new_models.empty:
        print("Sem modelos novos.")
        return
    
    with engine.connect() as connection:
        for index, row in new_models.iterrows():
            link = get_first_youtube_link(row['produto'], row['model'])
            try:
                connection.execute(
                    "INSERT INTO lakehouse.unique_models (produto, model, ids, links) VALUES (%s, %s, %s, %s)",
                    (row['produto'], row['model'], row['ids'], link)
                )
            except IntegrityError:
                continue

    print(f"{len(new_models)} novos modelos inseridos com sucesso.")
