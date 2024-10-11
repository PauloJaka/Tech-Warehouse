import re
import pandas as pd

def classify_product_with_regex(row):
    discount_price = row['discount_price'] if pd.notna(row['discount_price']) else row['original_price']
    
    if 'iphone' in row['title'].lower():
        return 'iPhone'
    
    ram_value_str = str(row['RAM'])
    ram_match = re.search(r'(\d+)', ram_value_str)
    ram_value = int(ram_match.group(1))

    if ram_value:
        if ram_value >= 6:
            return 'Gamer'
    
    if discount_price <= 1800 and ram_value >= 4:
        return 'Casual'
    
    return 'Outros'
