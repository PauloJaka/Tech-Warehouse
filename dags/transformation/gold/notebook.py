import pandas as pd
import re

def classify_product_with_regex(row):
    discount_price = row['discount_price'] if pd.notna(row['discount_price']) else row['original_price']
    
    intel_high_regex = r'\b(i[579]|core\s*i[579])[\s-]?\d+'
    ryzen_high_regex = r'\b(ryzen\s*[579]|r[579])[\s-]?\d*'
    cpu_str = str(row['CPU']).lower()

    def extract_ram_value(ram_value_str):
        ram_match = re.search(r'(\d+)', ram_value_str)
        return int(ram_match.group(1)) if ram_match else None

    ram_value = extract_ram_value(str(row['RAM']))
    ssd_value_str = str(row['SSD'])
    ssd_match = re.search(r'(\d+)', ssd_value_str)

    if pd.notna(row['GPU']):
        return 'Gamer'
    
    if re.search(intel_high_regex, cpu_str) or re.search(ryzen_high_regex, cpu_str):
        if ram_value and ssd_match:
            ssd_value = int(ssd_match.group(1))

            if ram_value >= 8 and ssd_value >= 256:
                return 'Gamer'

    if discount_price < 2500 and ram_value >= 8:
        return 'Casual'
    
    if 'Macbook' in str(row['title']):
        return 'Macbook'
    
    return 'Outros'

def categorize_cpu(row):
    cpu_str = str(row['cpu']).lower()
    intel_regex = r'\b(i[3579]|core\s*i[3579])[\s-]?\d+'
    ryzen_regex = r'\b(ryzen\s*([3579])|r([3579]))[\s-]?\d*'
    celeron_regex = r'\b(celeron|pentium|atom)'
    other_basic_regex = r'\b(dual\s*core|quad\s*core|amd\s*a[4-6]|amd\s*athlon)'

    intel_match = re.search(intel_regex, cpu_str)
    if intel_match:
        intel_model = intel_match.group(1)  
        return f'Intel {intel_model.upper()}'
    
    ryzen_match = re.search(ryzen_regex, cpu_str)
    if ryzen_match:
        ryzen_model = ryzen_match.group(2) or ryzen_match.group(3) 
        return f'Ryzen R{ryzen_model}'
    
    if re.search(celeron_regex, cpu_str):
        return 'Celeron/Pentium/Atom'
    
    if re.search(other_basic_regex, cpu_str):
        return 'Basic CPU (Dual/Quad Core, Athlon, A4-A6)'
    return 'Unknown CPU'

def apply_specifics_categorys_on_notebook(df: pd.DataFrame) -> pd.DataFrame:
    if df is not None:
        df['specifics'] = df.apply(classify_product_with_regex, axis=1)
        df['cpu_category'] = df.apply(categorize_cpu, axis=1)
    
    return df