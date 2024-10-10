from ingestion.ingestion_raw_and_bronze import insert_into_dimension, insert_into_fact_bronze, get_database_connection

def process_tables_bronze() -> None:
    engine = get_database_connection()
    
    dimension_tables: dict[str, list[str]] = {
        "d_bronze_smartphone": ["Smartphone"],
        "d_bronze_tablets": ["Tablet", "Ipad"],
        "d_bronze_notebooks": ["Notebook"],
        "d_bronze_tv": ["TV"],
        "d_bronze_smartwatch": ["Smartwatch"]
    } 
    insert_into_fact_bronze(engine)
    
    for table, categories in dimension_tables.items():
        insert_into_dimension(engine, table, categories)