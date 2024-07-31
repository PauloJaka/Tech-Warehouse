def process_data():
    # Caminho para o arquivo CSV de dados extraídos
    input_file = '/path/to/scraped_data.csv'
    #output_file = '/path/to/processed_data.csv'

    # Ler o arquivo CSV com os dados extraídos
    df = pd.read_csv(input_file)

    # Exemplo de processamento de dados:
    # Adicionar uma nova coluna calculada com base no preço
    df['price'] = df['price'].str.replace(',', '.').astype(float)  # Convertendo preços para float
    df['new_column'] = df['price'] * 1.1  # Exemplo de cálculo, como adicionar 10% ao preço

    # Salvar o DataFrame processado em um novo arquivo CSV
    # df.to_csv(output_file, index=False)

    print(df)