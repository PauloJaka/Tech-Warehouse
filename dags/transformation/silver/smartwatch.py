import pandas as pd

def apply_ner_to_smartwatch_title(df, nlp) -> pd.DataFrame:
    def entities_to_dataframe(text, doc):
        entities = {
            'model': ''
        }
        for ent in doc.ents:
            if ent.label_ == 'MODEL':
                entities['model'] = ent.text
        return entities
    
    new_entities = []
    for title in df['title']:
        doc = nlp(title)
        entities = entities_to_dataframe(title, doc)
        new_entities.append(entities)

    df_entities = pd.DataFrame(new_entities)
    df = pd.concat([df, df_entities], axis=1)
    df = df[(df['id'].notna()) & (df['id'] != '')]
    print(len(df))
    return df