import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from utils.utils import known_brands, MAX_THREADS
import re
from concurrent.futures import ThreadPoolExecutor
import os

def collect_data_from_page(url, current_product, known_brands):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    products = []
    product_elements = soup.select(".ui-search-result__wrapper")
    
    if not product_elements:
        print("No product elements found.")
    
    for item in product_elements:
        try:
            link_elementText = item.select_one('a')
            original_price_element = item.select_one(".andes-money-amount__fraction")  
            rating_element = item.select_one(".ui-search-reviews__rating-number")
            free_freight_element = item.select_one(".ui-pb-highlight-content .ui-pb-highlight")
            discount_price_element = item.select_one(".ui-search-installments-prefix span") 
            
            title = link_elementText.text.strip() if link_elementText else "No title"
            original_price = original_price_element.text.strip() if original_price_element else None
            rating = rating_element.text.strip() if rating_element else None
            free_freight = free_freight_element and "Frete grátis" in free_freight_element.text
            link_element = item.select_one("a.ui-search-link")
            if not link_element:
                link_element = item.select_one("h2.poly-box.poly-component__title a")
            link = link_element['href'] if link_element else None

            if discount_price_element:  
                discount_price_text = discount_price_element.text.strip()  
                discount_price = re.findall(r'\d+\.?\d*', discount_price_text)
                discount_price = discount_price[0] if discount_price else None
            else:
                discount_price = None

            brand = "Unknown"
            for known_brand in known_brands:
                if known_brand.lower() in title.lower():
                    brand = known_brand
                    break
            
            discount_price = discount_price.replace('.', '').replace(',', '.') if discount_price else None
            original_price = original_price.replace('.', '').replace(',', '.') if original_price else None

            products.append({
                'title': title,
                'discount_price': discount_price, 
                'original_price': original_price, 
                'brand': brand,
                'link': link,
                'rating': rating,
                'free_freight': free_freight,
                'category': current_product,
                'created_at': (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                'website': 'Mercado Livre'
            })
        except Exception as e:
            print(f"Error: {e}")
            continue
    return products

def scrape_products_page(base_url, current_product, num_pages=1):
    all_products = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:  
        futures = []
        for page in range(1, num_pages + 1):
            url = f"{base_url}_Desde_{(page-1)*50 + 1}"
            futures.append(executor.submit(collect_data_from_page, url, current_product, known_brands))
        
        for future in futures:
            all_products.extend(future.result())
    
    return pd.DataFrame(all_products)

def Mercado_Livre_Scrappy_Products():
    products = ["Notebook", "Smartphone", "TV", "Tablet", "Ipad", "Smartwatch"]  
    all_data = pd.DataFrame()
    num_pages = 8  

    for product in products:
        base_url = f"https://lista.mercadolivre.com.br/{product}"
        df = scrape_products_page(base_url, product, num_pages)
        all_data = pd.concat([all_data, df], ignore_index=True)

    if all_data.empty:
        print("Empty data")
    else:
        print(all_data.to_string(index=False))
        print(len(all_data))
        
    return all_data

if __name__ == "__main__":
    Mercado_Livre_Scrappy_Products()
