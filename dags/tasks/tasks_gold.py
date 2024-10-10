from ingestion.ingestion_gold import  move_data_silver_to_gold, get_new_data_for_gold,insert_data_into_gold_notebook
from transformation.gold.notebook import apply_specifics_categorys_on_notebook

def process_table_to_gold():
    move_data_silver_to_gold()
    
def process_table_to_gold_notebooks():
    df = get_new_data_for_gold('d_silver_notebooks', 'd_gold_notebooks')
    df = apply_specifics_categorys_on_notebook(df)
    insert_data_into_gold_notebook(df)