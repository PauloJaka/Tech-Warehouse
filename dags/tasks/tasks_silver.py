from ingestion.ingestion_silver import move_data_bronze_to_silver, insert_data_into_silver_notebook, insert_data_into_silver_tv, insert_data_into_silver_smartwatch, insert_data_into_silver_tablets, insert_data_into_silver_smartphone, get_new_data_for_silver
from dags.transformation.silver.notebook import  apply_ner_to_notebook_title, load_ner_model
from dags.transformation.silver.tv import apply_ner_to_tv_title
from dags.transformation.silver.smartwatch import apply_ner_to_smartwatch_title
from dags.transformation.silver.tablet import apply_ner_to_tablets_title
from dags.transformation.silver.smartphone import apply_ner_to_smartphones_title

def process_table_to_silver():
    move_data_bronze_to_silver()

def process_table_to_silver_notebooks():
    df = get_new_data_for_silver('d_bronze_notebooks', 'd_silver_notebooks')
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_notebooks'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_notebook_title(df, nlp)
    insert_data_into_silver_notebook(df)
    
def process_table_to_silver_tv():
    df = get_new_data_for_silver('d_bronze_tv', 'd_silver_tv')
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_tvs'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_tv_title(df, nlp)
    insert_data_into_silver_tv(df)
    
def process_table_to_silver_smartwatch():
    df = get_new_data_for_silver('d_bronze_smartwatch', 'd_silver_smartwatch')
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_smartwatches'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_smartwatch_title(df, nlp)
    insert_data_into_silver_smartwatch(df)
    
def process_table_to_silver_tablets():
    df = get_new_data_for_silver('d_bronze_tablets', 'd_silver_tablets')
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_tablets'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_tablets_title(df, nlp)
    insert_data_into_silver_tablets(df)
    
def process_table_to_silver_smartphone():
    df = get_new_data_for_silver('d_bronze_smartphone', 'd_silver_smartphone')
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_celulares'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_smartphones_title(df, nlp)
    insert_data_into_silver_smartphone(df)