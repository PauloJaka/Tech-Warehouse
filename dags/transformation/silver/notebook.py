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


def apply_ner_to_notebook_title(df, nlp) -> pd.DataFrame:
    def entities_to_dataframe(text, doc):
        entities = {
            'model': '',
            'CPU': '',
            'GPU': '',
            'RAM': '',
            'SSD': ''
        }
        for ent in doc.ents:
            if ent.label_ == 'MODEL':
                entities['model'] = ent.text
            elif ent.label_ == 'CPU':
                entities['CPU'] = ent.text
            elif ent.label_ == 'GPU':
                entities['GPU'] = ent.text
            elif ent.label_ == 'RAM':
                entities['RAM'] = ent.text
            elif ent.label_ == 'SSD':
                entities['SSD'] = ent.text
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
        doc = nlp(title)
        entities = entities_to_dataframe(title, doc)
        new_entities.append(entities)

    df_entities = pd.DataFrame(new_entities)
    df = pd.concat([df, df_entities], axis=1)
    df = remove_invalid(df)
    df = df[(df['id'].notna()) & (df['id'] != '')]
    
    return df