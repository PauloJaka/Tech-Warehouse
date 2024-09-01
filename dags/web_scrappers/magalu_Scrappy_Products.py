from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import pandas as pd
from datetime import datetime, timedelta
from utils.utils import known_brands
from bs4 import BeautifulSoup
import re

def initialize_driver(gecko_path, headless=True):
    print('driver')
    firefox_options = Options()
    if headless:
        firefox_options.add_argument("--headless")
    
    service = FirefoxService(executable_path=gecko_path)
    driver = webdriver.Firefox(service=service, options=firefox_options)
    return driver

def extract_product_info(soup, product_type):
    products = []
    ul_elements = soup.find_all('ul', class_=lambda c: c and c.startswith('sc-hknOHE'))
    
    if ul_elements:
        for ul_element in ul_elements:
            product_elements = ul_element.find_all('li', recursive=False)

            for item in product_elements:
                try:
                    title_element = item.select_one('h2[data-testid="product-title"]')
                    title = title_element.text.strip() if title_element else "Título não encontrado"
                    
                    link_element = item.select_one('a[data-testid="product-card-container"]')
                    link = link_element['href'] if link_element else "Link não encontrado"
                    link = f"https://www.magazineluiza.com.br{link}"
                    
                    price_original_element = item.select_one('p[data-testid="price-original"]')
                    price_original = price_original_element.text.strip().replace('R$', '').strip() if price_original_element else ""
                    
                    price_discount_element = item.select_one('p[data-testid="price-value"]')
                    price_discount = price_discount_element.text.strip().replace('R$', '').strip() if price_discount_element else ""
                    
                    review_element = item.select_one('span[format="score-count"]')
                    review_text = review_element.text.strip() if review_element else ""
                    review_match = re.match(r'([\d\.]+)', review_text)
                    review = review_match.group(1) if review_match else None
                    
                    brand = "Unknown"
                    for known_brand in known_brands:
                        if known_brand.lower() in title.lower():
                            brand = known_brand
                            break
                    discount_price = price_discount.replace('.', '').replace(',', '.') if price_discount else None
                    original_price = price_original.replace('.', '').replace(',', '.') if price_original else None
                    
                    products.append({
                        'title': title,
                        'link': link,
                        'original_price': original_price,
                        'discount_price': discount_price,
                        'rating': review,
                        'free_freight': False,
                        'brand': brand,
                        'category': product_type,
                        'created_at': (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                        'website': 'Magalu'
                    })
                
                except Exception as e:
                    print(f"Erro ao processar o produto: {e}")
                    continue
    
    return products

def scrape_magalu(gecko_path, base_url, product_type, num_pages=1, headless=True):
    driver = initialize_driver(gecko_path, headless)
    all_products = []

    for page in range(1, num_pages + 1):
        url = f"{base_url}?page={page}"
        driver.get(url)
        time.sleep(5)
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        products = extract_product_info(soup, product_type)
        all_products.extend(products)
    
    driver.quit()
    return all_products

def scrape_product_pages(product, gecko_path):
    base_url = f"https://www.magazineluiza.com.br/busca/{product}/"
    return scrape_magalu(gecko_path, base_url, product, num_pages=9, headless=True)

def Magalu_Scrappy_Products():
    start_time = time.time()
    
    gecko_path = os.getenv('Driver')
    products_list = ["Notebook", "Smartphone", "TV", "Tablet", "Ipad", "Smartwatch"]  
    max_threads = 3
    
    all_data = []

    with ThreadPoolExecutor(max_threads) as executor:
        futures = {executor.submit(scrape_product_pages, product, gecko_path): product for product in products_list}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                all_data.extend(result)
            except Exception as e:
                print(f"Error scraping product: {e}")

    df = pd.DataFrame(all_data)
    df = df[df['title'] != "Título não encontrado"]
    print(df.to_string(index=False))
    
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")
    return df

if __name__ == "__main__":
    Magalu_Scrappy_Products()