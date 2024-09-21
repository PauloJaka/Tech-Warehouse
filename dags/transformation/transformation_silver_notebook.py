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

def notebook_normalization(row):
    return sum(pd.isna(row[col]) or row[col] == '' for col in ['model', 'CPU', 'RAM', 'SSD'])


def load_ner_model(model_path):
    return spacy.load(model_path)


def apply_ner_to_notebook_title(df, nlp):
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
    
    def remove_invalid_ssd(df):
        return df[df['SSD'].apply(lambda x: len(str(x)) <= 6)]
    def remove_invalid_ram(df):
        return df[df['RAM'].apply(lambda x: len(str(x)) <= 5)]
    
    new_entities = []
    for title in df['title']:
        doc = nlp(title)
        entities = entities_to_dataframe(title, doc)
        new_entities.append(entities)

    df_entities = pd.DataFrame(new_entities)
    df = pd.concat([df, df_entities], axis=1)
    df['RAM'] = df['RAM'].apply(normalize_storage)
    df['SSD'] = df['SSD'].apply(normalize_storage)
    
    df = remove_invalid_ssd(df)
    df = remove_invalid_ram(df)
    df = df[df.apply(notebook_normalization, axis=1) != 3]
    
    return df