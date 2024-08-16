from ingestion_data import ingest_data_to_postgres

def generic_scrape_task(scrape_func, **kwargs):
    df = scrape_func()
    kwargs['ti'].xcom_push(key='scraped_df', value=df)

def generic_ingest_task(table_name, **kwargs):
    df = kwargs['ti'].xcom_pull(key='scraped_df', task_ids=kwargs['task_id'])
    ingest_data_to_postgres(df, table_name)
