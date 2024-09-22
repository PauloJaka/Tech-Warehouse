import pandas as pd
from .notebook import normalize_storage

def apply_ner_to_tablets_title(df, nlp) -> pd.DataFrame:
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
        doc = nlp(title)
        entities = entities_to_dataframe(title, doc)
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
    print(len(df))
    return df