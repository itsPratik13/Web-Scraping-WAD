from pinecone import Pinecone, ServerlessSpec
from datetime import datetime, timedelta

# Initialize Pinecone
print("Initializing Pinecone...")
pinecone = Pinecone(
    api_key="pcsk_6zhFRD_9HjM2kGQsiRQoYPZb4bJXpnkrdSEFGkwpyEeXUBrEWcadLXyhgdNnreT3zJPXwy",  # Replace with your actual API key
    environment="us-west1-gcp"  # Replace with your actual environment
)
print("Pinecone initialized successfully.")

# List of categories and their corresponding indexes
CATEGORIES = ["tech", "sports", "entertainment", "social media", "stocks"]

def ensure_indexes_exist():
    """Ensure that indexes for all categories exist in Pinecone."""
    print("Checking existing indexes in Pinecone...")
    existing_indexes = pinecone.list_indexes()
    print(f"Existing indexes: {existing_indexes}")
    
    for category in CATEGORIES:
        index_name = f"{category}-index"
        if index_name not in existing_indexes:
            print(f"Index '{index_name}' does not exist. Creating it...")
            pinecone.create_index(
                name=index_name,
                dimension=1536,  # Adjust dimension based on your embedding model
                metric="cosine",  # Use the appropriate metric for your use case
                spec=ServerlessSpec(cloud="gcp", region="us-west1")  # Change to a supported region
            )
            print(f"Created index: {index_name}")
        else:
            print(f"Index '{index_name}' already exists.")

def get_namespace(category):
    """Return the namespace based on the category."""
    namespace = category.lower()
    print(f"Namespace for category '{category}': {namespace}")
    return namespace

def upload_to_pinecone(data, category):
    """Upload data to Pinecone within a specific index and namespace."""
    try:
        print("Preparing to upload data to Pinecone...")
        ensure_indexes_exist()  # Ensure indexes exist before uploading

        # Define the namespace based on the category
        namespace = category.lower()
        index_name = f"{category}-index"

        print(f"Index name: {index_name}")
        print(f"Namespace: {namespace}")
        print(f"Data to upload: {data}")

        # Prepare the vectors for upserting
        vectors = [
            {
                "id": record["id"],
                "values": record["values"],  # Embedding vector
                "metadata": record["metadata"]  # Metadata (e.g., title, category, date, URL)
            }
            for record in data
        ]
        print(f"Vectors prepared for upsert: {vectors}")

        # Get the index
        index = pinecone.Index(index_name)

        # Upsert the vectors into the namespace
        index.upsert(vectors=vectors, namespace=namespace)
        print(f"Data uploaded to Pinecone in the '{category}' index and '{namespace}' namespace.")
    except Exception as e:
        print(f"Error uploading data to Pinecone: {e}")

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

def test_pinecone_insertion():
    """Test inserting dummy data into Pinecone."""
    try:
        print("Testing Pinecone insertion with dummy data...")

        # Define the category and namespace
        category = "test"
        namespace = get_namespace(category)
        index_name = "test-index"  # Use an existing index

        # Define the correct dimension (e.g., 1024)
        dimension = 1024

        # Prepare dummy data with the correct dimension
        dummy_data = [
            {
                "id": "dummy-id-1",
                "values": [0.1] * dimension,  # Generate a vector with 1024 dimensions
                "metadata": {
                    "title": "Dummy Title 1",
                    "category": "test",
                    "date": "2025-04-05T00:00:00",
                    "url": "https://example.com/dummy1"
                }
            },
            {
                "id": "dummy-id-2",
                "values": [0.2] * dimension,  # Generate a vector with 1024 dimensions
                "metadata": {
                    "title": "Dummy Title 2",
                    "category": "test",
                    "date": "2025-04-05T00:00:00",
                    "url": "https://example.com/dummy2"
                }
            }
        ]

        print(f"Dummy data to upload: {dummy_data}")

        # Get the index
        index = pinecone.Index(index_name)

        # Upsert the dummy data into the namespace
        index.upsert(vectors=dummy_data, namespace=namespace)
        print(f"Dummy data uploaded to Pinecone in the '{category}' index and '{namespace}' namespace.")
    except Exception as e:
        print(f"Error testing Pinecone insertion: {e}")

if __name__ == "__main__":
    test_pinecone_insertion()
