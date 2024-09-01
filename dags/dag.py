from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tasks.tasks import run_amazon_scrapy_and_ingest, run_mercado_livre_scrapy_and_ingest, run_magalu_scrapy_and_ingest

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Scrape_Multiple_Sites',
    default_args=default_args,
    description='Uma DAG para fazer scraping de múltiplos sites e ingestão dos dados',
    schedule_interval='@once',
)

#run_amazon_scrapy_and_ingest = PythonOperator(
#    task_id='run_amazon_scrapy_and_ingest',
#    python_callable=run_amazon_scrapy_and_ingest,
#    provide_context=True,
#    dag=dag
#)

run_mercado_livre_scrapy_and_ingest = PythonOperator(
    task_id='run_mercado_livre_scrapy_and_ingest',
    python_callable=run_mercado_livre_scrapy_and_ingest,
    provide_context=True,
    dag=dag,
)

#run_magalu_scrapy_and_ingest = PythonOperator(
#    task_id='run_magalu_scrapy_and_ingest',
#    python_callable=run_magalu_scrapy_and_ingest,
#    provide_context=True,
#    dag=dag,
#)
