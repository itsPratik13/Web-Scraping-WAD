import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from src.utils import log_info
from src.pinecone_manager import upload_to_pinecone
from src.processor import process_data  # Import process_data from processor.py

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
        category = selectors.get('category', 'default')  # Use category from selectors if available

        pub_date = None
        try:
            pub_date = datetime.strptime(date, '%Y-%m-%d') if date else datetime.now()
        except ValueError:
            pub_date = datetime.now()

        scraped_data = {
            "title": title,
            "content": content,
            "date": pub_date,
            "category": category,
            "url": url
        }
        log_info(f"Scraped data: {scraped_data}")  # Debug log
        return scraped_data
    except requests.exceptions.RequestException as e:
        log_info(f"Error scraping {url}: {e}")
        return None


def scrape_and_upload_data():
    all_scraped_data = []
    for category, websites in config['websites'].items():
        for website in websites:
            log_info(f"Scraping {website['url']} in category {category}")
            scraped_data = scrape_site(website['url'], website['selectors'])

            if scraped_data:
                log_info(f"Scraped data: {scraped_data}")
                processed_data = process_data(scraped_data)  # Use process_data from processor.py
                if processed_data:
                    log_info(f"Processed data: {processed_data}")
                    all_scraped_data.append(processed_data)

                    # Upload to Pinecone based on category
                    upload_to_pinecone([processed_data])
                else:
                    log_info(f"Failed to process data for {website['url']}")
            else:
                log_info(f"Failed to scrape data for {website['url']}")
    return all_scraped_data