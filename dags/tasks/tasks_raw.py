from ingestion.ingestion_data import ingest_data_to_postgres, get_database_connection, get_existing_data, update_existing_data
from dags.web_scrappers.amazon_Scrappy_Products import Amazon_Scrappy_Products
from dags.web_scrappers.mercado_livre_Scrappy_Products import Mercado_Livre_Scrappy_Products
from dags.web_scrappers.magalu_Scrappy_Products import Magalu_Scrappy_Products
from dags.web_scrappers.americanas_Scrappy_Products import Americanas_Scrappy_Products
from dags.web_scrappers.casas_bahia_Scrappy_Products import Casas_Bahia_Scrappy_Products
from dags.web_scrappers.kalunga_Scrappy_Products import Kalunga_Scrappy_Products
from dags.web_scrappers.fastshop_Scrappy_Products import FastShop_Scrappy_Products
from dags.web_scrappers.kabum_Scrappy_Products import KaBum_Scrappy_Products

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
    
def run_americanas_scrapy_and_ingest():
    engine = get_database_connection()

    df = Americanas_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para Americanas:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para Americanas está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para Americanas é vazio ou None")
    
def run_casas_bahia_scrapy_and_ingest():
    engine = get_database_connection()

    df = Casas_Bahia_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para Casas Bahia:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para Casas Bahia está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para Casas Bahia é vazio ou None")
    
def run_kalunga_scrapy_and_ingest():
    engine = get_database_connection()

    df = Kalunga_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para Kalunga:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para Kalunga está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para Kalunga é vazio ou None")

def run_fastshop_scrapy_and_ingest():
    engine = get_database_connection()

    df = FastShop_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para fastshop:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para fastshop está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para fastshop é vazio ou None")

def run_kabum_scrapy_and_ingest():
    engine = get_database_connection()

    df = KaBum_Scrappy_Products()
    if df is not None and not df.empty:
        print(f"DataFrame para KaBum:")
        
        table_name = "raw"
        existing_data = get_existing_data(engine)
        df = update_existing_data(df, existing_data)
        ingest_data_to_postgres(df, table_name)
    else:
        print(f"Erro: DataFrame para KaBum está vazio ou não foi gerado corretamente.")
        raise ValueError(f"DataFrame para KaBum é vazio ou None")

