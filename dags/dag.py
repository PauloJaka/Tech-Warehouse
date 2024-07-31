from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/dags/scripts')

from Amazon_webScrappyNotebook import Amazon_Scrappy_Notebook

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

scrape_task = PythonOperator(
    task_id='Scrape_Amazon',
    python_callable=Amazon_Scrappy_Notebook,
    dag=dag,
)

