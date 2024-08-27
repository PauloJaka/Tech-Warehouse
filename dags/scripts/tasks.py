from ingestion_data import ingest_data_to_postgres, get_database_connection, get_existing_data, update_existing_data
from Amazon_Scrappy_Products import Amazon_Scrappy_Products
from mercado_livre_Scrappy_Products import Mercado_Livre_Scrappy_Products
from magalu_Scrappy_Products import Magalu_Scrappy_Products

def run_amazon_scrapy_and_ingest():
    engine = get_database_connection()

    df = Amazon_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para Amazon:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para Amazon está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para Amazon é vazio ou None")
    
def run_mercado_livre_scrapy_and_ingest():
    engine = get_database_connection()

    df = Mercado_Livre_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para Mercado Livre:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para Mercado Livre está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para Mercado Livre é vazio ou None")
    
def run_magalu_scrapy_and_ingest():
    engine = get_database_connection()

    df = Magalu_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para Magalu:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para Magalu está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para Magalu é vazio ou None")
