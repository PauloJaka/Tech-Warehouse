from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/dags/scripts')

from Amazon_Scrappy_Products import Amazon_Scrappy_Products
from tasks import generic_scrape_task, generic_ingest_task

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

scraping_functions = {
    'amazon': Amazon_Scrappy_Products
    #'mercado': Mercado_Scrappy_Products,
}

for site, scrape_func in scraping_functions.items():
    scrape_task = PythonOperator(
        task_id=f'scrape_{site}',
        python_callable=generic_scrape_task,
        op_kwargs={'scrape_func': scrape_func},
        provide_context=True,
        dag=dag,
    )

    ingest_task = PythonOperator(
        task_id=f'ingest_{site}',
        python_callable=generic_ingest_task,
        op_kwargs={'table_name': 'raw'},
        provide_context=True,
        dag=dag,
    )

    scrape_task >> ingest_task
