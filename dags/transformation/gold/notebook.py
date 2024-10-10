import pandas as pd
import re

def classify_product_with_regex(row):
    discount_price = row['discount_price'] if pd.notna(row['discount_price']) else row['original_price']
    
    intel_high_regex = r'\b(i[579]|core\s*i[579])[\s-]?\d+'
    ryzen_high_regex = r'\b(ryzen\s*[579]|r[579])[\s-]?\d*'
    intel_low_regex = r'\b(i3|core\s*i3)[\s-]?\d+'
    ryzen_low_regex = r'\b(ryzen\s*3|r3)[\s-]?\d*'
    celeron_regex = r'\b(celeron|pentium|atom)'
    other_basic_regex = r'\b(dual\s*core|quad\s*core|amd\s*a[4-6]|amd\s*athlon)'

    if pd.notna(row['GPU']):
        return 'Gamer'
    
    cpu_str = str(row['CPU']).lower()

    if re.search(intel_high_regex, cpu_str) or re.search(ryzen_high_regex, cpu_str):
        ram_value_str = str(row['RAM'])  
        ssd_value_str = str(row['SSD'])  

        ram_match = re.search(r'(\d+)', ram_value_str)
        ssd_match = re.search(r'(\d+)', ssd_value_str)

        if ram_match and ssd_match:
            ram_value = int(ram_match.group(1)) 
            ssd_value = int(ssd_match.group(1)) 

            if ram_value > 8 and ssd_value >= 256:
                return 'Gamer'

    if discount_price < 3000 and '8GB' in str(row['RAM']):
        if (re.search(intel_low_regex, cpu_str) or 
            re.search(ryzen_low_regex, cpu_str) or 
            re.search(celeron_regex, cpu_str) or 
            re.search(other_basic_regex, cpu_str)):
            return 'Casual'
    
    if 'Macbook' in str(row['titulo']):
        return 'Macbook'
    
    return 'Outros'

def categorize_cpu(row):
    cpu_str = str(row['cpu']).lower()
    intel_regex = r'\b(i[3579]|core\s*i[3579])[\s-]?\d+'
    ryzen_regex = r'\b(ryzen\s*[3579]|r[3579])[\s-]?\d*'
    celeron_regex = r'\b(celeron|pentium|atom)'
    other_basic_regex = r'\b(dual\s*core|quad\s*core|amd\s*a[4-6]|amd\s*athlon)'

    if re.search(intel_regex, cpu_str):
        return 'Intel (i3/i5/i7/i9)'
    elif re.search(ryzen_regex, cpu_str):
        return 'Ryzen (R3/R5/R7/R9)'
    elif re.search(celeron_regex, cpu_str):
        return 'Celeron/Pentium/Atom'
    elif re.search(other_basic_regex, cpu_str):
        return 'Basic CPU (Dual/Quad Core, Athlon, A4-A6)'
    else:
        return 'Unknown CPU'

def apply_specifics_categorys_on_notebook(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None:
        df['specifics'] = df.apply(classify_product_with_regex, axis=1)
        df['cpu_category'] = df.apply(categorize_cpu, axis=1)
    
    return df