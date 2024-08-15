from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/dags/scripts')

from Amazon_Scrappy_Products import Amazon_Scrappy_Products
from ingestion_data import ingest_data_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Scrape_Amazon',
    default_args=default_args,
    description='Uma DAG para fazer scraping da Amazon',
    schedule_interval='@once',
)

def run_scrapy_and_ingest():
    df = Amazon_Scrappy_Products()
    ingest_data_to_postgres(df, 'raw')

scrape_and_ingest_task = PythonOperator(
    task_id='Scrape_Amazon_and_Ingest',
    python_callable=run_scrapy_and_ingest,
    dag=dag,
)
