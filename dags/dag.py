from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags/scripts')
from tasks import run_scrapy_and_ingest_in_raw

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

run_scrapy_and_ingest_task = PythonOperator(
    task_id='run_scrapy_and_ingest',
    python_callable=run_scrapy_and_ingest_in_raw,
    provide_context=True,
    dag=dag,
)
