import re
import pandas as pd

def classify_product_with_regex(row):
    discount_price = row['discount_price'] if pd.notna(row['discount_price']) else row['original_price']
    
    if 'iphone' in row['title'].lower():
        return 'iPhone'
    
    ram_value_str = str(row['ram'])
    ram_match = re.search(r'(\d+)', ram_value_str)
    
    if ram_match:
        ram_value = int(ram_match.group(1)) 
    else:
        ram_value = None  

    if ram_value is not None:
        if ram_value >= 6:
            return 'Gamer'
        
        if discount_price <= 1800 and ram_value >= 4:
            return 'Casual'
        
    if discount_price <= 1200:
        return 'Reserva'
    
    return 'Outros'

def apply_specifics_categorys_on_smartphone(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None:
        df['specifics'] = df.apply(classify_product_with_regex, axis=1)
    return df
