import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.ingestion_silver import move_data_bronze_to_silver, load_ner_model, insert_data_into_silver_notebook, get_data_from_dimension, apply_ner_to_title

def process_table_to_silver():
    
    move_data_bronze_to_silver()
    dimension_table_for_data =  'd_bronze_notebooks'
    dimension_table_for_insert = 'd_silver_notebooks' 
    df = get_data_from_dimension(dimension_table_for_data)
    print(df)
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_notebooks'  
    nlp = load_ner_model(model_path)

    # Passo 4: Aplicar NER aos títulos
    df = apply_ner_to_title(df, nlp)
    print(df)
    insert_data_into_silver_notebook(df, dimension_table_for_insert)