import pandas as pd

def apply_ner_to_tv_title(df, nlp) -> pd.DataFrame:
    def entities_to_dataframe(text, doc):
        entities = {
            'model': '',
            'size': '',
            'resolution': '',
            'technology': ''
        }
        for ent in doc.ents:
            if ent.label_ == 'SIZE':
                entities['size'] = ent.text
            elif ent.label_ == 'MODEL': 
                entities['model'] = ent.text
            elif ent.label_ == 'RESOLUTION':
                entities['resolution'] = ent.text
            elif ent.label_ == 'TECHNOLOGY':
                entities['technology'] = ent.text
        return entities
    
    # Função para garantir que o tamanho seja numérico
    def remove_invalid_size(df):
        df['size'] = df['size'].apply(lambda x: str(x) if str(x).isdigit() and len(str(x)) <= 4 else None)
        return df

    
    
    new_entities = []
    for title in df['title']:
        doc = nlp(title)
        entities = entities_to_dataframe(title, doc)
        new_entities.append(entities)

    df_entities = pd.DataFrame(new_entities)
    print(df_entities)
    df = pd.concat([df, df_entities], axis=1)
    df = remove_invalid_size(df)
    print(len(df))
    return df