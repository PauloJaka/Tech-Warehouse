import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.ingestion_silver import move_data_bronze_to_silver, insert_data_into_silver_notebook, get_data_from_dimension
from dags.transformation.silver.notebook import  apply_ner_to_notebook_title, load_ner_model

def process_table_to_silver():
    move_data_bronze_to_silver()

def process_table_to_silver_notebooks():
    df = get_data_from_dimension('d_bronze_notebooks')
    print(len(df))
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_notebooks'  
    nlp = load_ner_model(model_path)
    df = apply_ner_to_notebook_title(df, nlp)
    print(len(df))
    insert_data_into_silver_notebook(df)