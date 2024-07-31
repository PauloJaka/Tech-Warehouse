def Amazon_Scrappy_Notebook():
    
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

    
    firefox_options = Options()
    service = FirefoxService(executable_path='/usr/local/bin/geckodriver')
    firefox_options.binary_location = '/opt/firefox/firefox'
    driver = webdriver.Firefox(service=service, options=firefox_options)

    base_url = "https://www.amazon.com.br/s?k=notebook"

    
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
                continue
        return products

    num_pages = 1
    all_products = []

    for page in range(1, num_pages + 1):
        url = f"{base_url}&page={page}"
        driver.get(url)

        time.sleep(5)

        products = collect_data_from_page()
        all_products.extend(products)


    driver.quit()

    
    df = pd.DataFrame(all_products)

    
    print(df.to_string(index=False))

if __name__ == "__main__":
    Amazon_Scrappy_Notebook()
