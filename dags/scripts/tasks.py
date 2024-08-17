from ingestion_data import ingest_data_to_postgres, get_database_connection, get_existing_data, update_existing_data
from Amazon_Scrappy_Products import Amazon_Scrappy_Products

SCRAPE_FUNCTIONS = {
    'Amazon': Amazon_Scrappy_Products
   # 'mercado': Mercado_Scrappy_Products,
    # Adicione outros sites aqui
}

def run_scrapy_and_ingest_in_raw():
    engine = get_database_connection()

    for site, scrape_func in SCRAPE_FUNCTIONS.items():
        df = scrape_func()

        if df is not None and not df.empty:
            print(f"DataFrame para {site}:")
            
            table_name = "raw"

            existing_data = get_existing_data(engine)

            df = update_existing_data(df, existing_data)

            ingest_data_to_postgres(df, table_name)
        else:
            print(f"Erro: DataFrame para {site} está vazio ou não foi gerado corretamente.")
            raise ValueError(f"DataFrame para {site} é vazio ou None")
