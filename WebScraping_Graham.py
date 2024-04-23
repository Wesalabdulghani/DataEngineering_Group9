# loading libraries
from bs4 import BeautifulSoup
import requests
from lxml import etree
import pandas as pd
import csv
from time import sleep
import time
from PyPDF2 import PdfReader
from io import BytesIO
import re
import fitz
import io
import toml
import os
from dotenv import load_dotenv
import subprocess
import configparser

# Create a ConfigParser object
config = configparser.ConfigParser()
with open('config_file.toml', 'r') as config_file:
    config = toml.load(config_file)

# Accessing the configurations
api_url = config['api']['api_url']
s3_bucket = config['s3']['bucket']
s3_folder = config['s3']['folder']
py_script = config['run']['py_script']



# Parsing URLs
def get_watches_collections(main_url):
    main_html = requests.get(main_url)
    main_soup = BeautifulSoup(main_html.content, "html.parser")
    
    # Parsing the page with the watches
    for a in main_soup.find_all("a", href=True):
        if "find my graham" in str(a).lower():
            break
    url = main_url+str(a)[str(a).find("href=")+len("href="):str(a).find(">")].replace('"',"")
    html = requests.get(url)
    soup = BeautifulSoup(html.content, "html.parser")
    
    # Parsing the collections
    collections = soup.find_all("div", attrs={"class":"collection-sidebar__group--1"})[0].find_all("span", attrs={"class":"tag__text"})
    collections = [collection.text for collection in collections]
    
    watches_per_collection = {collection: [] for collection in collections}
    products_url = []
    
    # Parsing URLs
    for collection in collections:
        collection_url = url+"?filter.p.m.custom.collection="+collection.replace(" ", "+")
        collection_soup = BeautifulSoup(requests.get(collection_url).content, "html.parser")
        watches_element = collection_soup.find_all("div", attrs={"class": "collection-grid__wrapper"})[0].find_all("div", attrs={"class": "grid-product"})
        for i in range(len(watches_element)):
            products_url.append(main_url+watches_element[i].find('a').attrs["href"])
        watches_per_collection[collection].extend([watches_element[i].find("div", attrs={"class": "grid-product__title"}).text for i in range(len(watches_element))])
        # Looking for next pages
        next_page = collection_soup.find_all("span", attrs={"class": "page"})
        if next_page != []:
            for page in next_page[1:]:
                next_page_url = main_url+page.find("a").attrs["href"]
                next_page_soup = BeautifulSoup(requests.get(next_page_url).content, "html.parser")
                watches_element = next_page_soup.find_all("div", attrs={"class": "collection-grid__wrapper"})[0].find_all("div", attrs={"class": "grid-product"})
                for i in range(len(watches_element)):
                    products_url.append(main_url+watches_element[i].find('a').attrs["href"])
                watches_per_collection[collection].extend([watches_element[i].find("div", attrs={"class": "grid-product__title"}).text for i in range(len(watches_element))])
    
    return products_url, watches_per_collection

# Extracting Technical Sheet
def extract_text_from_pdf(url):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        pdf_stream = io.BytesIO(response.content)
        doc = fitz.open("pdf", pdf_stream)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        return text
    else:
        print(f"Failed to download the PDF from {url}")
        return ""

# Extracting Product Details
def scrape_product_details(url, watches_per_collection):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # Extract reference_number
        sku_element = soup.find('p', class_='product-single__sku')
        sku = sku_element.get_text(strip=True) if sku_element else None
        
        # Extract URL
        watch_url = url
        
        # Extract specific_model
        title = soup.find('h1', class_='product-single__title').get_text(strip=True)
        
        # Extract parent_model
        for collection in watches_per_collection.keys():
            if title in watches_per_collection[collection]:
                collection_name = collection
                break
        
        # Extract marketing_name
        marketing_name = None
        for h in soup.find_all(re.compile("^h[3-4]$")):
            if "LIMITED EDITION" in h.text.upper():
                marketing_name = h.text
                next_element = h.find_next()
                for i in range(3):
                    if 'pieces' in next_element.text:
                        marketing_name = marketing_name+'\n'+next_element.text
                        break
                    next_element = next_element.find_next()
                break
        
        # Extract currency & price
        price_text = soup.find('span', class_='product__price').get_text(strip=True)
        currency = 'USD' if price_text[0] == '$' else price_text[0]
        price = price_text[1:] if currency == 'USD' else price_text

        # Extract image URL
        image_element = soup.find('img', class_='photoswipe__image')
        image_url = 'https:' + image_element['data-photoswipe-src'] if image_element else 'No image found'
        
        # Extract short description URL
        technical_sheet_link = soup.find('a', string=lambda t: t and "TECHNICAL SHEET" in t)
        technical_sheet_url = technical_sheet_link['href'] if technical_sheet_link else None

        # Extract description
        description_element = soup.find('div', id='frc-106')
        description = ''
        if description_element:
            paragraphs = description_element.find_all('p')
            description = ' '.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        if not description:
            description_heading = soup.find(lambda tag: tag.name == "h3" and "DESCRIPTION" in tag.text)
            if description_heading:
                next_sibling = description_heading.find_next_sibling('p')
                while next_sibling:
                    description += next_sibling.get_text(strip=True) + ' '
                    next_sibling = next_sibling.find_next_sibling()
                    if next_sibling.name != 'p':
                            break
                if not next_sibling:
                    next_element = description_heading.find_next('div').find_next('p')
                    while next_element:
                        description += next_element.get_text(strip=True) + ' '
                        next_element = next_element.find_next()
                        if next_element.name != 'p':
                            break

        # PDF sheet
        technical_info = {}
        if technical_sheet_url:
            technical_sheet_text = extract_text_from_pdf(technical_sheet_url)
            upper_words = [upper_word for upper_word in technical_sheet_text.split(" ") if upper_word.isupper()]
            for upper_word in upper_words:
                technical_sheet_text = technical_sheet_text.replace(upper_word, "")
            regex_patterns = {
                'style': r"Functions:\s*(.+?)\s*(?=\()",
                'diameter': r"Case:[\s\S]*?(\d+\s*mm)",
                'bezel_material': r"Bezel:[^A-Za-z]*([A-Za-z]+)",
                'bezel_color': r"Bezel:\s*(.*?)(?=,|\.)",
                'water_resistance': r"Water\s+resistance:\s*([^\n]+)",
                'crystal': r"(.*?\bcrystal\b.*)",
                'frequency': r"(\d{2}’\d{3}\s*A/h\s*\(\d+Hz\))",
                'jewels': r"(\d+)\s*jewels",
                'dial_color': r"Dial:\s*([\w\s-]+)",
                'bracelet_color': r"Strap:\s*([\w\s-]+)\s*(?:pin buckle)",
                'bracelet_material': r"\b(\w+)\s+(?=\b(?:pin\s+)?buckle\b)",
                'clasp_type': r"(.*?\bpin buckle\b)",
                'movement': r"(Automatic|Manual|Quartz|Solar|Kinetic|Spring\s*Drive|Mechanical|Perpetual\s*Calendar|Tourbillon)",
                'calibre': r"Calibre:\s*(G\d{4})",
                'power_reserve': r"Power reserve:\s*(\d+\s*hours)",
                'case_material': r"\d+\s*mm,\s*([a-zA-Z\s]+)\scase"
            }
            
            for key, pattern in regex_patterns.items():
                match = re.search(pattern, technical_sheet_text)
                technical_info[key] = match.group(1) if match else None

        product_details = {
            'specific_model': title,
            'nickname':title,
            'reference_number': sku,
            'currency': currency,
            'price': price,
            'image_url': image_url,
            'marketing_name': marketing_name,
            'made_in': 'Switzerland',
            'brand': 'Graham',
            'description': description ,
            'watch_url': watch_url,
            'short_description': technical_sheet_url,
            'parent_model': collection_name,
            **technical_info
            }

        return product_details
    else:
        print(f"Failed to retrieve product details from {url}")
        return {}

# Saving To CSV File
def save_to_csv(details, filename):
    with open(filename, mode='a', newline='') as csvfile:
      fieldnames = [
        'reference_number', 'watch_url', 'type', 'brand', 'year_introduced',
        'parent_model', 'specific_model', 'nickname', 'marketing_name', 'style',
        'currency', 'price', 'image_url', 'made_in', 'case_shape',
        'case_material', 'case_finish', 'caseback', 'diameter', 'between_lugs',
        'lug_to_lug', 'case_thickness', 'bezel_material', 'bezel_color',
        'crystal', 'water_resistance', 'weight', 'dial_color', 'numerals',
        'bracelet_material', 'bracelet_color', 'clasp_type', 'movement',
        'calibre', 'power_reserve', 'frequency', 'jewels', 'features',
        'description', 'short_description']

      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

      if csvfile.tell() == 0:
            writer.writeheader()

      writer.writerow(details)

def upload_file_to_s3(file_path, bucket_name, s3_file_path):
    try:
        subprocess.run(["aws", "s3", "cp", file_path, f"s3://{bucket_name}/{s3_file_path}"], check=True)
        print(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_file_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {file_path} to {bucket_name}. Error: {e}")




if __name__ == '__main__':
    main_url = 'https://graham1695.com'
    filename = 'Graham_Srapes.csv'
    s3_bucket_name = config['s3']['bucket']  # Assuming you've loaded this from the config file
    s3_file_path = config['s3']['folder'] + filename  # Assuming folder path is in config and appending the filename
    upload_file_to_s3(filename, s3_bucket_name, s3_file_path)
    products_urls, watches_per_collection = get_watches_collections(main_url)
    for product_url in products_urls:
        product_details = scrape_product_details(product_url, watches_per_collection)
        save_to_csv(product_details, filename)
        # Sleep to prevent overwhelming
        time.sleep(1)

    # After saving the CSV, upload it to S3
    s3_file_path = s3_folder + filename  # Construct the full S3 file path
    upload_file_to_s3(filename, s3_bucket, s3_file_path)