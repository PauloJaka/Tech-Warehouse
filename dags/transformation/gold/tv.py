import re
import pandas as pd

def classify_product_with_regex(row):
    size_value = int(str(row['size']).strip())

    if size_value > 49:
        return 'Sala'
    elif 28 <= size_value <= 49:
        return 'Video games'
    else:
        return 'Monitor'

def apply_specifics_categorys_on_tv(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None:
        df['specifics'] = df.apply(classify_product_with_regex, axis=1)
    return df

