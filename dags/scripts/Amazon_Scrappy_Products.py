def Amazon_Scrappy_Products():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.firefox.options import Options
    import pandas as pd
    import time
    from datetime import datetime
    import os

    known_brands = [
        "ACER", "ASUS", "SAMSUNG", "Dell", "Positivo", "Lenovo", "VAIO",
        "HP", "Apple", "Multilaser", "Anvazise", "ASHATA", "Santino", "MSI",
        "Marca Fácil", "Microsoft", "AWOW", "Gateway", "Compaq", "DAUERHAFT",
        "SGIN", "Luqeeg", "Kiboule", "LG", "Panasonic", "Focket", "Toughbook",
        "LTI", "GIGABYTE", "Octoo", "Chip7 Informática", "GLOGLOW", "GOLDENTEC",
        "KUU", "HEEPDD", "Adamantiun", "Naroote", "Jectse", "Heayzoki", "Galaxy",
        "Motorola", "Xiaomi", "Nokia", "Poco", "realme", "Infinix", "Blu",
        "Gshield", "Geonav", "Redmi", "Gorila Shield", "intelbras", "TCL",
        "Tecno", "Vbestlife", "MaiJin", "SZAMBIT", "Otterbox", "Sony"
    ]

    firefox_options = Options()
    service = FirefoxService(executable_path='/usr/local/bin/geckodriver')
    firefox_options.binary_location = '/opt/firefox/firefox'
    driver = webdriver.Firefox(service=service, options=firefox_options)

    def collect_data_from_page(product_type):
        products = []
        product_elements = driver.find_elements(By.CSS_SELECTOR, ".s-main-slot .s-result-item")

        for item in product_elements:
            try:
                title_element = item.find_element(By.CSS_SELECTOR, "h2 a span")
                price_element = item.find_element(By.CSS_SELECTOR, ".a-price-whole")
                rating_element = item.find_element(By.CSS_SELECTOR, ".a-icon-alt")
                link_element = item.find_element(By.CSS_SELECTOR, "a.a-link-normal.s-underline-text.s-underline-link-text.s-link-style.a-text-normal")
                original_price_element = item.find_element(By.CSS_SELECTOR, ".a-price.a-text-price .a-offscreen")
                
                try:
                    free_freight = item.find_element(By.CSS_SELECTOR, "span[aria-label='Opção de frete GRÁTIS disponível']")
                    free_freight = True
                except:
                    free_freight = False

                link = link_element.get_attribute("href") if link_element else "No link"
                title = title_element.text
                discount_price = price_element.text
                rating = rating_element.get_attribute("innerHTML").split()[0] if rating_element else "No rating"
                original_price = original_price_element.text if original_price_element else ""
                
                discount_price = discount_price.replace('.', '').replace(',', '.') if discount_price else None
                original_price = original_price.replace('.', '').replace(',', '.') if original_price else None
                rating = rating.replace('.', '').replace(',', '.') if discount_price else None

                brand = next((b for b in known_brands if b.lower() in title.lower()), "Unknown")

                products.append({
                    'title': title,
                    'price_discount': discount_price,
                    'price_original': original_price,
                    'brand': brand,
                    'rating': rating,
                    'link': link,
                    'free_freight': free_freight,
                    'category': product_type,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'website': 'Amazon'
                })
            except Exception as e:
                continue
        return products

    products_list = ["Notebook"]#, "Smartphone", "TV", "Tablet", "Ipad", "Smartwatch"]  
    num_pages = 1
    all_products = []

    for product in products_list:
        for page in range(1, num_pages + 1):
            url = f"https://www.amazon.com.br/s?k={product}&page={page}"
            driver.get(url)

            time.sleep(5)

            products = collect_data_from_page(product)
            all_products.extend(products)

    driver.quit()

    df = pd.DataFrame(all_products)
    print(df.to_string(index=False))
    
    return df

if __name__ == "__main__":
    Amazon_Scrappy_Products()
