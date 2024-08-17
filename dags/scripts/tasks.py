from ingestion_data import ingest_data_to_postgres
from Amazon_Scrappy_Products import Amazon_Scrappy_Products

SCRAPE_FUNCTIONS = {
    'amazon': Amazon_Scrappy_Products
   # 'mercado': Mercado_Scrappy_Products,
    # Adicione outros sites aqui
}

def run_scrapy_and_ingest_in_raw():
    for site, scrape_func in SCRAPE_FUNCTIONS.items():

        df = scrape_func()
        
        if df is not None and not df.empty:
            print(f"DataFrame para {site}:")
            print(df.to_string(index=False))
            
            table_name = "raw"
            
            ingest_data_to_postgres(df, table_name)
        else:
            print(f"Erro: DataFrame para {site} está vazio ou não foi gerado corretamente.")
            raise ValueError(f"DataFrame para {site} é vazio ou None")
