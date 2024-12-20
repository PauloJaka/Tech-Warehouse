from airflow import DAG
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.tasks_bronze import process_tables_bronze
from tasks.tasks_gold import process_table_unique_products


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'Scrape_Multiple_Sites',
    default_args=default_args,
    description='Uma DAG para fazer scraping de múltiplos sites e ingestão dos dados',
    schedule_interval='@once'
)


def scrapy_failure_callback(context):
    print(f"Task error: {context['task_instance'].task_id}")
    # Adicione aqui qualquer outra lógica, como envio de e-mails, registro de logs, etc.

def create_scraping_task(dag:DAG, task_id: str, scraping_function_name: str, on_failure_callback=None, trigger_rule='none_skipped'):
    def scraping_task_callable():
        module = __import__('dags.tasks.tasks_raw', fromlist=[scraping_function_name])
        scraping_function = getattr(module, scraping_function_name)
        scraping_function()

    return PythonOperator(
        task_id=task_id,
        python_callable=scraping_task_callable,
        on_failure_callback=on_failure_callback,
        trigger_rule=trigger_rule,
        dag=dag
    )

with TaskGroup(group_id='scrapy_group', dag=dag) as scrapy_group:
    amazon_task = create_scraping_task(dag, 'amazon', 'run_amazon_scrapy_and_ingest', on_failure_callback=scrapy_failure_callback)
    mercado_livre_task = create_scraping_task(dag, 'mercado_livre', 'run_mercado_livre_scrapy_and_ingest')
    magalu_task = create_scraping_task(dag, 'magalu', 'run_magalu_scrapy_and_ingest')
    americanas_task = create_scraping_task(dag, 'americanas', 'run_americanas_scrapy_and_ingest')
    casas_bahia_task = create_scraping_task(dag, 'casas_bahia', 'run_casas_bahia_scrapy_and_ingest')
    kalunga_task = create_scraping_task(dag, 'kalunga', 'run_kalunga_scrapy_and_ingest')
    fastshop_task = create_scraping_task(dag,'fastshop','run_fastshop_scrapy_and_ingest')
    kabum_task = create_scraping_task(dag,'kaBum', 'run_kabum_scrapy_and_ingest')

    amazon_task >> mercado_livre_task >> magalu_task >> americanas_task >> casas_bahia_task >> kalunga_task >> fastshop_task >> kabum_task # pyright: ignore [reportUnusedExpression]
scrapy_group # pyright: ignore [reportUnusedExpression]

process_tables_bronze_task = PythonOperator(
    task_id='process_tables_bronze',
    python_callable=process_tables_bronze,
    dag=dag
)

def create_silver_insert_batch(dag: DAG, task_id: str, silver_table_insert_function: str, on_failure_callback=None, trigger_rule='all_done'):
    def insert_silver_callable():
        module = __import__('dags.tasks.tasks_silver', fromlist=[silver_table_insert_function])
        scraping_function = getattr(module, silver_table_insert_function)
        scraping_function()

    return PythonOperator(
        task_id=task_id,
        python_callable=insert_silver_callable,
        on_failure_callback=on_failure_callback,
        trigger_rule=trigger_rule,
        dag=dag
    )

with TaskGroup(group_id='silver_insert_group', dag=dag) as silver_insert_group:
    silver_fact_table = create_silver_insert_batch(dag, 'silver_fact', 'process_table_to_silver')
    silver_notebook = create_silver_insert_batch(dag, 'notebook_dimension', 'process_table_to_silver_notebooks')
    silver_tv = create_silver_insert_batch(dag, 'tv_dimension', 'process_table_to_silver_tv')
    silver_smartwach = create_silver_insert_batch(dag, 'smartwatch_dimension', 'process_table_to_silver_smartwatch')
    silver_tablet = create_silver_insert_batch(dag, 'tablet_dimension', 'process_table_to_silver_tablets')
    silver_smartphone = create_silver_insert_batch(dag, 'smartphone_dimension', 'process_table_to_silver_smartphone')

    silver_fact_table >>  silver_notebook >> silver_tv >> silver_smartwach >> silver_tablet >> silver_smartphone # pyright: ignore [reportUnusedExpression]
silver_insert_group # pyright: ignore [reportUnusedExpression]

def create_gold_insert_batch(dag, task_id, gold_table_insert_function, on_failure_callback=None, trigger_rule='all_done'):
    def insert_gold_callable():
        module = __import__('dags.tasks.tasks_gold', fromlist=[gold_table_insert_function])
        scraping_function = getattr(module, gold_table_insert_function)
        scraping_function()

    return PythonOperator(
        task_id=task_id,
        python_callable=insert_gold_callable,
        on_failure_callback=on_failure_callback,
        trigger_rule=trigger_rule,
        dag=dag
    )
        
with TaskGroup(group_id='gold_insert_group', dag=dag) as gold_insert_group:
    gold_fact_table = create_gold_insert_batch(dag, 'gold_fact', 'process_table_to_gold')
    gold_notebook = create_gold_insert_batch(dag, 'notebook_dimension', 'process_table_to_gold_notebooks')
    gold_tv = create_gold_insert_batch(dag, 'tv_dimension', 'process_table_to_gold_tv')
    gold_smartwach = create_gold_insert_batch(dag, 'smartwatch_dimension', 'process_table_to_gold_smartwatch')
    gold_tablet = create_gold_insert_batch(dag, 'tablet_dimension', 'process_table_to_gold_tablets')
    gold_smartphone = create_gold_insert_batch(dag, 'smartphone_dimension', 'process_table_to_gold_smartphone')

    gold_fact_table >>  gold_notebook >> gold_tv >> gold_smartwach >> gold_tablet >> gold_smartphone # pyright: ignore [reportUnusedExpression]
gold_insert_group # pyright: ignore [reportUnusedExpression]

process_table_unique_products_func = PythonOperator(
    task_id='insert_news_models',
    python_callable=process_table_unique_products,
    dag=dag
)

scrapy_group >> process_tables_bronze_task >> silver_insert_group >> gold_insert_group >> process_table_unique_products_func# pyright: ignore [reportUnusedExpression]