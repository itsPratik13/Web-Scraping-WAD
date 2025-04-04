import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from src.utils import log_info
from src.pinecone_manager import upload_to_pinecone

with open('config/config.json') as f:
    config = json.load(f)

def scrape_site(url, selectors):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        title = soup.select_one(selectors['title']).get_text()
        content = soup.select_one(selectors['content']).get_text()
        date = soup.select_one(selectors['date']).get_text()
        category = soup.select_one(selectors['category']).get_text()

        pub_date = None
        try:
            pub_date = datetime.strptime(date, '%Y-%m-%d') if date else datetime.now()
        except ValueError:
            pub_date = datetime.now()     

        return {
            "title": title,
            "content": content,
            "date": pub_date,
            "category": category,
            "url": url
        } 
    except requests.exceptions.RequestException as e:
        log_info(f"Error scraping {url} : {e}")


def scrape_and_upload_data():
    all_scraped_data = []
    for category, websites in config['websites'].items():
        for website in websites:
            log_info(f"Scraping {website['url']} in category {category}")
            scraped_data = scrape_site(website['url'], website['selectors'])
            
            if scraped_data:
                processed_data = process_data(scraped_data)
                all_scraped_data.append(processed_data)
                
                # Upload to Pinecone based on category
                upload_to_pinecone([processed_data], category)
    return all_scraped_data