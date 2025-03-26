import openai
import hashlib
from src.utils import log_info

openai.api_key = 'your-openai-api-key'

def generate_embeddings(text):
    """Generate text embeddings using OpenAI's API."""
    response = openai.Embedding.create(input=[text], model="text-embedding-ada-002")
    return response['data'][0]['embedding']

def create_unique_id(url, pub_date):
    """Create a unique ID for each article."""
    hash_object = hashlib.md5(f"{url}{pub_date}".encode())
    return hash_object.hexdigest()

def process_data(scraped_data):
    """Process scraped data and prepare it for Pinecone upload."""
    embedding = generate_embeddings(scraped_data['content'])
    unique_id = create_unique_id(scraped_data['url'], scraped_data['date'])
    
    metadata = {
        "title": scraped_data["title"],
        "category": scraped_data["category"],
        "date": scraped_data["date"].isoformat(),
        "url": scraped_data["url"]
    }
    
    log_info(f"Processed data for {scraped_data['title']}")
    
    return {
        "id": unique_id,
        "values": embedding,
        "metadata": metadata
    }
