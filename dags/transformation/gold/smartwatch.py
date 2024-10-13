import re
import pandas as pd

def classify_product_with_regex(row):
    discount_price = row['discount_price'] if pd.notna(row['discount_price']) else row['original_price']
    
    if 'nfc' in row['title'].lower():
        return 'NFC'

    if discount_price <= 399:
        return 'Casual'

def apply_specifics_categorys_on_smartwach(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None:
        df['specifics'] = df.apply(classify_product_with_regex, axis=1)
    return df

