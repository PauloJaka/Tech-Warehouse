import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.ingestion_silver import move_data_bronze_to_silver, load_ner_model, insert_data_into_silver, get_data_from_dimension, apply_ner_to_title

def process_table_to_silver():
    # Passo 1: Mover dados da tabela bronze para a tabela silver
    move_data_bronze_to_silver()

    # Passo 2: Obter dados da dimensão da tabela silver (por exemplo, para notebooks)
    dimension_table_for_data =  'd_bronze_notebooks'# ou outra tabela específica
    dimension_table_for_insert = 'd_silver_notebooks' 
    df = get_data_from_dimension(dimension_table_for_data)
    print(df)
    # Passo 3: Carregar o modelo NER
    model_path = '/opt/airflow/dags/utils/trained_models/modelo_ner_notebooks'  # Atualize com o caminho correto
    nlp = load_ner_model(model_path)

    # Passo 4: Aplicar NER aos títulos
    df = apply_ner_to_title(df, nlp)
    print(df)
    # Passo 5: Inserir dados processados na tabela silver
    insert_data_into_silver(df, dimension_table_for_insert)