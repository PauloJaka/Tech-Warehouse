from sqlalchemy import create_engine
import pandas as pd

def ingest_data_to_postgres(df, table_name):
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')    

    try:
        df.to_sql(table_name, engine, schema='lakehouse', if_exists='append', index=False)
        print(f"Dados inseridos na tabela {table_name} com sucesso.")
    except Exception as e:
        print(f"Ocorreu um erro ao inserir os dados: {e}")
