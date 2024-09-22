import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.ingestion_silver import move_data_bronze_to_silver, insert_data_into_silver_notebook, get_data_from_dimension, insert_data_into_silver_tv
from dags.transformation.silver.notebook import  apply_ner_to_notebook_title, load_ner_model
from dags.transformation.silver.tv import apply_ner_to_tv_title

def process_table_to_silver():
    move_data_bronze_to_silver()

def process_table_to_silver_notebooks():
    df = get_data_from_dimension('d_bronze_notebooks')
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_notebooks'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_notebook_title(df, nlp)
    print(len(df))
    insert_data_into_silver_notebook(df)
    
    
def process_table_to_silver_tv():
    df = get_data_from_dimension('d_bronze_tv')
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_tvs'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_tv_title(df, nlp)
    print(len(df))
    insert_data_into_silver_tv(df)