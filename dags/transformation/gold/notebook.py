import re
import pandas as pd

def extract_ram_value(ram_value_str):
    ram_match = re.search(r'(\d+)', ram_value_str)
    return int(ram_match.group(1)) if ram_match else None

def classify_product_with_regex(row):
    discount_price = row.get('discount_price', row.get('original_price'))
    
    intel_high_regex = r'\b(i[579]|core\s*i[579])[\s-]?\d+'
    ryzen_high_regex = r'\b(ryzen\s*[579]|r[579])[\s-]?\d*'
    
    cpu_str = str(row.get('cpu', '')).lower() if pd.notna(row.get('cpu')) else None

    ram_value = extract_ram_value(str(row.get('ram', '')))
    ssd_value_str = str(row.get('ssd', ''))
    ssd_value = int(re.search(r'(\d+)', ssd_value_str).group(1)) if re.search(r'(\d+)', ssd_value_str) else None

    if pd.notna(row.get('gpu')) and str(row.get('gpu', '')).strip() != '':
        return 'Gamer'

    if cpu_str and (re.search(intel_high_regex, cpu_str) or re.search(ryzen_high_regex, cpu_str)):
        if ram_value is not None and ssd_value is not None:
            if ram_value >= 8 and ssd_value >= 256:
                return 'Gamer-Without GPU'
    
    if ram_value is not None and discount_price is not None and discount_price < 2500 and ram_value >= 8:
        return 'Casual'
    
    if 'macbook' in str(row.get('title', '')).lower():
        return 'Macbook'
    
    return 'Outros'

def categorize_cpu(row):
    cpu_str = str(row.get('cpu', '')).lower() if pd.notna(row.get('cpu')) else None
    
    if not cpu_str:
        return None

    intel_regex = r'\b(core\s*i[3579]|i[3579])\s*\d*'
    ryzen_regex = r'\b(ryzen\s*([3579])|r([3579]))\s*\d*'
    celeron_regex = r'\b(celeron|pentium|atom)'
    other_basic_regex = r'\b(dual\s*core|quad\s*core|amd\s*a[4-6]|amd\s*athlon)'

    if re.search(intel_regex, cpu_str):
        intel_matches = re.findall(intel_regex, cpu_str)
        intel_models = [match.strip().capitalize() for match in intel_matches]
        return ', '.join(set(intel_models))  
    
    if re.search(ryzen_regex, cpu_str):
        ryzen_model = re.search(ryzen_regex, cpu_str)
        return f'Ryzen R{ryzen_model.group(2) or ryzen_model.group(3)}'  # Retorna o modelo correto
    
    if re.search(celeron_regex, cpu_str):
        return 'Celeron/Pentium/Atom'
    
    if re.search(other_basic_regex, cpu_str):
        return 'Basic CPU (Dual/Quad Core, Athlon, A4-A6)'
    
    return 'Unknown CPU'



def apply_specifics_categorys_on_notebook(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica as classificações e categorização às colunas do DataFrame."""
    if df is not None and not df.empty:
        df['specifics'] = df.apply(classify_product_with_regex, axis=1)
        df['cpu_category'] = df.apply(categorize_cpu, axis=1)
    return df
