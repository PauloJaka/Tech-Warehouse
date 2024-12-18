from ingestion.ingestion_gold import  execute_query_to_dataframe ,move_data_silver_to_gold, get_new_data_for_gold,insert_data_into_gold_notebook, insert_data_into_gold_tv, insert_data_into_gold_smartphone, insert_data_into_silver_tablets, insert_data_into_gold_smartwatch
from transformation.gold.notebook import apply_specifics_categorys_on_notebook
from transformation.gold.tv import apply_specifics_categorys_on_tv
from transformation.gold.smartphone import apply_specifics_categorys_on_smartphone 
from transformation.gold.tablet import apply_specifics_categorys_on_tablet
from transformation.gold.smartwatch import apply_specifics_categorys_on_smartwach
from transformation.gold.get_links import insert_new_models
from utils.aditional_querys_gold.notebook import notebook_query
from utils.aditional_querys_gold.tv import tv_query
from utils.aditional_querys_gold.smartphone import smartphone_query
from utils.aditional_querys_gold.tablets import tablet_query
from utils.aditional_querys_gold.smartwatch import smartwatch_query
from utils.aditional_querys_gold.unique_products import unique_products

def process_table_to_gold():
    move_data_silver_to_gold()
    
def process_table_to_gold_notebooks():
    df = get_new_data_for_gold('d_silver_notebooks', 'd_gold_notebooks', notebook_query)
    df = apply_specifics_categorys_on_notebook(df)
    insert_data_into_gold_notebook(df)
    
def process_table_to_gold_tv():
    df = get_new_data_for_gold('d_silver_tv', 'd_gold_tv', tv_query)
    df = apply_specifics_categorys_on_tv(df)
    insert_data_into_gold_tv(df)

def process_table_to_gold_smartphone():
    df = get_new_data_for_gold('d_silver_smartphone', 'd_gold_smartphone', smartphone_query)
    df = apply_specifics_categorys_on_smartphone(df)
    insert_data_into_gold_smartphone(df)

def process_table_to_gold_tablets():
    df = get_new_data_for_gold('d_silver_tablets', 'd_gold_tablets', tablet_query)
    df = apply_specifics_categorys_on_tablet(df)
    insert_data_into_silver_tablets(df)
    
def process_table_to_gold_smartwatch():
    df = get_new_data_for_gold('d_silver_smartwatch', 'd_gold_smartwatch', smartwatch_query)
    df = apply_specifics_categorys_on_smartwach(df)
    insert_data_into_gold_smartwatch(df)


def process_table_unique_products():
    df = execute_query_to_dataframe(unique_products)
    insert_new_models(df)

