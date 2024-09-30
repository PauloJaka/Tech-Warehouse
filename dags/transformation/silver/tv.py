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
    
    def remove_invalid_size(df):
        df['size'] = df['size'].apply(lambda x: str(x) if str(x).isdigit() and len(str(x)) <= 4 else None)
        return df
    
    new_entities = []
    
    for title in df['title']:
        attempts = 0
        entities = {}
        
        while attempts < 5 and (len(entities) < 3):  
            doc = nlp(title)
            entities = entities_to_dataframe(title, doc)
            attempts += 1

        if not entities['size']:
            if entities['model']:
                entities['size'] = entities['model'][:2]  

        new_entities.append(entities)

    df_entities = pd.DataFrame(new_entities)
    print(df_entities)
    df = pd.concat([df, df_entities], axis=1)
    df = remove_invalid_size(df)
    df = df[(df['id'].notna()) & (df['id'] != '')]
    print(len(df))
    return df
