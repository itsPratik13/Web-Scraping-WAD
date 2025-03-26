import schedule
import time
from src.scraper import scrape_and_upload_data
from src.pinecone_manager import delete_old_data

def job():
    """Run the full pipeline."""
    scrape_and_upload_data()
    delete_old_data(retention_days=15)

schedule.every().day.at("00:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
