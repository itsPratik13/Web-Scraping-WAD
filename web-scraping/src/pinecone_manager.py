import pinecone
from datetime import datetime, timedelta

pinecone.init(api_key="your-pinecone-api-key", environment="us-west1-gcp")

def get_namespace(category):
    """Return the namespace based on the category."""
    return category.lower()

def upload_to_pinecone(data, category):
    """Upload data to Pinecone within a specific namespace."""
    namespace = get_namespace(category)
    
    batch = [{"id": entry['id'], "values": entry['values'], "metadata": entry['metadata']} for entry in data]
    
    index = pinecone.Index("news-index")
    index.upsert(vectors=batch, namespace=namespace)
    print(f"Data uploaded to Pinecone in the {category} namespace.")

def delete_old_data(retention_days=15, category=None):
    """Delete vectors older than retention period from Pinecone."""
    retention_cutoff = datetime.now() - timedelta(days=retention_days)
    cutoff_timestamp = int(retention_cutoff.timestamp())
    
    namespace = get_namespace(category) if category else None

    query = {"filter": {"date": {"$lt": cutoff_timestamp}}}
    index = pinecone.Index("news-index")
    results = index.query(query, namespace=namespace)
    
    ids_to_delete = [result['id'] for result in results['matches']]

    if ids_to_delete:
        index.delete(ids=ids_to_delete, namespace=namespace)
        print(f"Deleted {len(ids_to_delete)} vectors from the {category} namespace.")
    else:
        print(f"No vectors older than {retention_days} days in the {category} namespace.")
