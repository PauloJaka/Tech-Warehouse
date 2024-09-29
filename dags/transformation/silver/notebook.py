import pandas as pd
import re
import spacy

def normalize_storage(value):
    if pd.isna(value):
        return value
    numbers = re.findall(r'\d+', str(value))
    if numbers:
        number = int(numbers[0])
        if number == 1:
            return f"{number}TB"
        else:
            return f"{number}GB"
    return value


def load_ner_model(model_path):
    return spacy.load(model_path)

def extract_macbook_info(text):
    match = re.search(r'macbook\s+(\w+).*?\b(m\d+)\b', text, re.IGNORECASE)
    if match:
        model = f"{match.group(1).lower()} {match.group(2).lower()}"  
        return model
    return None

def extract_ram(title):
    match = re.search(r"(\d+)\s*GB\s*RAM", title, re.IGNORECASE)
    if match:
        return match.group(1) + " GB"
    return None

# Função auxiliar para identificar SSD
def extract_storage_capacity(title):
    match = re.search(r"(\d+)\s*GB", title, re.IGNORECASE)
    if match and int(match.group(1)) > 128:
        return match.group(1) + " GB"
    return None

def apply_ner_to_notebook_title(df, nlp, max_attempts=3) -> pd.DataFrame:
    def entities_to_dataframe(text):
        entities = {
            'model': '',
            'CPU': '',
            'GPU': '',
            'RAM': '',
            'SSD': ''
        }

        def try_fill_entity(entity_key, extraction_func, *args):
            if not entities[entity_key]:
                result = extraction_func(*args)
                if result:
                    entities[entity_key] = result

        attempts = max_attempts
        while attempts > 0:
            # Tenta usar o modelo NER
            doc = nlp(text)
            for ent in doc.ents:
                if ent.label_ == 'MODEL' and not entities['model']:
                    entities['model'] = ent.text
                elif ent.label_ == 'CPU' and not entities['CPU']:
                    entities['CPU'] = ent.text
                elif ent.label_ == 'GPU' and not entities['GPU']:
                    entities['GPU'] = ent.text
                elif ent.label_ == 'RAM' and not entities['RAM']:
                    entities['RAM'] = ent.text
                elif ent.label_ == 'SSD' and not entities['SSD']:
                    entities['SSD'] = ent.text

            # Segunda tentativa: tentar extrair o modelo "MacBook"
            try_fill_entity('model', extract_macbook_info, text)
            try_fill_entity('CPU', extract_macbook_info, text)
            try_fill_entity('RAM', extract_ram, text)
            try_fill_entity('SSD', extract_storage_capacity, text)

            if entities['model'] and entities['CPU'] and entities['RAM'] and entities['SSD']:
                break

            attempts -= 1

        if not entities['RAM']:
            match = re.search(r"(\d+)\s*GB", text, re.IGNORECASE)
            if match and int(match.group(1)) in [4, 8, 16, 32]:
                if re.search(r"RAM", text, re.IGNORECASE):
                    entities['RAM'] = match.group(1) + " GB"

        if not entities['SSD']:
            match = re.search(r"(\d+)\s*GB", text, re.IGNORECASE)
            if match and int(match.group(1)) > 128:
                if re.search(r"SSD", text, re.IGNORECASE):
                    entities['SSD'] = match.group(1) + " GB"

        return entities

    def remove_invalid(df):
        df['SSD'] = df['SSD'].apply(lambda x: x if len(str(x)) <= 6 else None)
        df['CPU'] = df['CPU'].apply(lambda x: x if len(str(x)) <= 40 else None)
        df['RAM'] = df['RAM'].apply(lambda x: x if len(str(x)) <= 5 else None)
        df['model'] = df['model'].apply(lambda x: x if len(str(x)) <= 40 else None)
        df['GPU'] = df['GPU'].apply(lambda x: x if len(str(x)) <= 40 else None)
        return df    

    new_entities = []
    for title in df['title']:
        entities = entities_to_dataframe(title)
        new_entities.append(entities)

    df_entities = pd.DataFrame(new_entities)
    df = pd.concat([df, df_entities], axis=1)
    df = remove_invalid(df)
    df['SSD'] = df['SSD'].apply(normalize_storage)
    df['RAM'] = df['RAM'].apply(normalize_storage)
    df = df[(df['id'].notna()) & (df['id'] != '')]
    
    return df
