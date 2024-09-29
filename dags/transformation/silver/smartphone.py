import pandas as pd
import re
from .notebook import normalize_storage

def extract_ram(title):
    match = re.search(r"(\d+)\s*GB\s*RAM", title, re.IGNORECASE)
    if match:
        return match.group(1) + " GB"
    return None

def extract_storage_capacity(title):
    match = re.search(r"(\d+)\s*GB", title, re.IGNORECASE)
    if match and int(match.group(1)) > 16:
        return match.group(1) + " GB"
    return None

def apply_ner_to_smartphones_title(df, nlp, max_attempts=5) -> pd.DataFrame:
    def entities_to_dataframe(text, doc):
        entities = {
            'model': '',
            'RAM': '',
            'storage_capacity': ''
        }

        for ent in doc.ents:
            if ent.label_ == 'MODEL':
                entities['model'] = ent.text
            elif ent.label_ == 'RAM':
                entities['RAM'] = ent.text
            elif ent.label_ == 'STORAGE':
                entities['storage_capacity'] = ent.text

        return entities
    
    new_entities = []
    for title in df['title']:
        entities = {
            'model': '',
            'RAM': '',
            'storage_capacity': ''
        }

        attempts = 0
        while attempts < max_attempts:
            doc = nlp(title)
            entities = entities_to_dataframe(title, doc)

            if not entities['RAM']:
                extracted_ram = extract_ram(title)
                if extracted_ram:
                    entities['RAM'] = extracted_ram
            
            if not entities['storage_capacity']:
                extracted_storage = extract_storage_capacity(title)
                if extracted_storage:
                    entities['storage_capacity'] = extracted_storage
            
            if entities['model'] or entities['RAM'] or entities['storage_capacity']:
                break

            attempts += 1
        
        new_entities.append(entities)

    def replace_invalid_storage_capacity(df):
        df['storage_capacity'] = df['storage_capacity'].apply(lambda x: x if len(str(x)) <= 6 else None)
        return df
    
    def replace_invalid_ram(df):
        df['RAM'] = df['RAM'].apply(lambda x: x if len(str(x)) <= 5 else None)
        return df

    def replace_invalid_model(df):
        df['model'] = df['model'].apply(lambda x: x if len(str(x)) <= 30 else None)
        return df

    df_entities = pd.DataFrame(new_entities)
    print(df_entities)
    df = pd.concat([df, df_entities], axis=1)
    df['RAM'] = df['RAM'].apply(normalize_storage)
    df['storage_capacity'] = df['storage_capacity'].apply(normalize_storage)
    df = replace_invalid_storage_capacity(df)
    df = replace_invalid_ram(df)
    df = replace_invalid_model(df)
    df = df[(df['id'].notna()) & (df['id'] != '')]
    print(len(df))
    return df
