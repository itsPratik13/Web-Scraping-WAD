from pinecone import Pinecone, ServerlessSpec
from datetime import datetime, timedelta

# Initialize Pinecone
print("Initializing Pinecone...")
pinecone = Pinecone(
    api_key="your-pinecone-api-key",  # Replace with your actual API key
    environment="us-west1-gcp"        # Replace with your actual environment
)
print("Pinecone initialized successfully.")

# List of categories and their corresponding indexes
CATEGORIES = ["tech", "sports", "entertainment", "social media", "stocks"]

def ensure_indexes_exist():
    """Ensure that indexes for all categories exist in Pinecone."""
    print("Checking existing indexes in Pinecone...")
    existing_indexes = [index.name for index in pinecone.list_indexes()]
    print(f"Existing indexes: {existing_indexes}")
    
    for category in CATEGORIES:
        index_name = f"{category}-index"
        if index_name not in existing_indexes:
            print(f"Index '{index_name}' does not exist. Creating it...")
            pinecone.create_index(
                name=index_name,
                dimension=1536,  # Adjust dimension based on your embedding model
                metric="cosine",  # Use the appropriate metric for your use case
                spec=ServerlessSpec(cloud="aws", region="us-west-2")  # Adjust cloud and region as needed
            )
            print(f"Created index: {index_name}")
        else:
            print(f"Index '{index_name}' already exists.")

def get_namespace(category):
    """Return the namespace based on the category."""
    namespace = category.lower()
    print(f"Namespace for category '{category}': {namespace}")
    return namespace

def upload_to_pinecone(data):
    """Upload data to Pinecone within a specific index and namespace."""
    print("Preparing to upload data to Pinecone...")
    ensure_indexes_exist()  # Ensure indexes exist before uploading
    
    for record in data:
        category = record.get("category", "default").lower()  # Extract category from the record
        index_name = f"{category}-index"
        namespace = get_namespace(category)
        
        print(f"Index name: {index_name}, Namespace: {namespace}")
        print(f"Data to upload: {record}")
        
        # Prepare the batch for upserting
        batch = [{
            "id": record.get("id", f"{category}-{datetime.now().timestamp()}"),
            "values": record['values'],  # Ensure 'values' contains the embedding vector
            "metadata": record
        }]
        print(f"Batch prepared for upload: {batch}")
        
        # Upsert into the appropriate index
        index = pinecone.index(index_name)
        index.upsert(vectors=batch, namespace=namespace)
        print(f"Data uploaded to Pinecone in the '{category}' index and '{namespace}' namespace.")

def delete_old_data(retention_days=15, category=None):
    """Delete vectors older than retention period from Pinecone."""
    print(f"Preparing to delete old data older than {retention_days} days...")
    retention_cutoff = datetime.now() - timedelta(days=retention_days)
    cutoff_timestamp = int(retention_cutoff.timestamp())
    print(f"Retention cutoff timestamp: {cutoff_timestamp}")
    
    namespace = get_namespace(category) if category else None
    index_name = f"{category}-index" if category else None

    if index_name:
        print(f"Querying index '{index_name}' for old data...")
        index = pinecone.index(index_name)
        query = {"filter": {"date": {"$lt": cutoff_timestamp}}}
        results = index.query(query, namespace=namespace)
        
        ids_to_delete = [result['id'] for result in results['matches']]
        print(f"IDs to delete: {ids_to_delete}")

        if ids_to_delete:
            index.delete(ids=ids_to_delete, namespace=namespace)
            print(f"Deleted {len(ids_to_delete)} vectors from the '{category}' index and namespace.")
        else:
            print(f"No vectors older than {retention_days} days in the '{category}' index and namespace.")
