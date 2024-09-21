from airflow import DAG
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.tasks_bronze import process_tables_bronze
#from tasks.tasks_silver import process_table_to_silver


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
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

def create_scraping_task(dag, task_id, scraping_function_name, on_failure_callback=None, trigger_rule='none_skipped'):
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

def run_process_tables_bronze():
    process_tables_bronze()

process_tables_bronze_task = PythonOperator(
    task_id='process_tables_bronze',
    python_callable=run_process_tables_bronze,
    dag=dag
)

scrapy_group >> process_tables_bronze_task # pyright: ignore [reportUnusedExpression]


#dagTask = DAG(
#    'test_tasks_dag',
#    default_args=default_args,
#    description='DAG para testar a função tasks',
#    schedule_interval=None,  # Altere conforme necessário
#    catchup=False,
#)
#
#test_tasks_operator = PythonOperator(
#    task_id='run_silver_batch',
#    python_callable=process_table_to_silver,
#    dag=dagTask,
#)
#
## Definir o fluxo de tarefas
#test_tasks_operator  # pyright: ignore [reportUnusedExpression]