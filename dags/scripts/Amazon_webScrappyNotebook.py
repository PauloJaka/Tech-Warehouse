# Amazon_webScrappy.py
def Amazon_Scrappy_Notebook():
    # Seu código de scraping aqui
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.firefox.options import Options
    import pandas as pd
    import time
    from datetime import datetime

    known_brands = ["ACER", "ASUS", "SAMSUNG", "Dell", "Positivo", "Lenovo", "VAIO", "HP", "Apple", "Multilaser",
                    "Anvazise", "ASHATA", "Santino", "MSI", "Marca Fácil", "Microsoft", "AWOW", "Gateway", "Compaq",
                    "DAUERHAFT", "SGIN", "Luqeeg", "Kiboule", "LG", "Panasonic", "Focket", "Toughbook", "LTI",
                    "GIGABYTE", "Octoo", "Chip7 Informática", "GLOGLOW", "GOLDENTEC", "KUU", "HEEPDD", "Adamantiun",
                    "Naroote", "Jectse", "Heayzoki"]

    # Configurações do Selenium para Firefox
    firefox_options = Options()
    # firefox_options.add_argument("--headless")  # Executar o navegador em modo headless (sem interface gráfica)

    # Inicializar o driver do Firefox
    service = FirefoxService(executable_path='/usr/local/bin/geckodriver')
    firefox_options.binary_location = '/opt/firefox/firefox'
    driver = webdriver.Firefox(service=service, options=firefox_options)

    # URL base para scraping
    base_url = "https://www.amazon.com.br/s?k=notebook"

    # Função para coletar dados de uma página
    def collect_data_from_page():
        products = []
        product_elements = driver.find_elements(By.CSS_SELECTOR, ".s-main-slot .s-result-item")

        for item in product_elements:
            try:
                title_element = item.find_element(By.CSS_SELECTOR, "h2 a span")
                price_element = item.find_element(By.CSS_SELECTOR, ".a-price-whole")
                rating_element = item.find_element(By.CSS_SELECTOR, ".a-icon-alt")

                title = title_element.text
                price = price_element.text
                rating = rating_element.get_attribute("innerHTML").split()[0] if rating_element else "No rating"

                # Extrair a marca do título
                brand = next((b for b in known_brands if b in title), "Unknown")

                products.append({
                    'title': title,
                    'price': price,
                    'brand': brand,
                    'rating': rating,
                    'CreatedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'UpdatedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'website': 'Amazon'
                })
            except Exception as e:
                # Ignorar produtos que não possuem título ou preço
                continue
        return products

    # Número de páginas a serem coletadas
    num_pages = 1

    all_products = []

    for page in range(1, num_pages + 1):
        # Construir a URL da página
        url = f"{base_url}&page={page}"
        driver.get(url)

        # Esperar a página carregar
        time.sleep(5)

        # Coletar dados da página
        products = collect_data_from_page()
        all_products.extend(products)

    # Fechar o driver
    driver.quit()

    # Converter para DataFrame do Pandas
    df = pd.DataFrame(all_products)

    # Printar os dados
    print(df.to_string(index=False))

if __name__ == "__main__":
    Amazon_Scrappy_Notebook()
