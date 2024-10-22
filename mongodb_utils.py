import pymongo
from datetime import datetime
import bson.json_util as bson_json  # For exporting MongoDB data
import logging
import time
from retry import retry

# Logging setup
logging.basicConfig(filename='mongodb_utils.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB setup with retry for connection failures
@retry(tries=3, delay=5, backoff=2)
def get_mongo_client(uri="mongodb://localhost:27017/"):
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # Forces a call to check the server status
        return client
    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        raise

# Get a specific MongoDB collection
def get_collection(db_name, collection_name):
    client = get_mongo_client()
    if client:
        db = client[db_name]
        return db[collection_name]
    else:
        logging.error(f"Could not connect to MongoDB to get collection: {collection_name}")
    return None

# Function to insert metadata after processing a PDF
def insert_metadata(file_metadata, url):
    metadata = {
        "document_name": file_metadata['filename'],
        "size": file_metadata['size'],
        "url": url,
        "status": "uploaded",
        "timestamp": datetime.now()
    }
    try:
        collection = get_collection("pdf_database", "pdf_documents")
        if collection:
            collection.insert_one(metadata)
            logging.info(f"Inserted metadata for {file_metadata['filename']}")
    except Exception as e:
        logging.error(f"Error inserting metadata for {file_metadata['filename']}: {e}")

# Function to update MongoDB with processing results
def update_document(file_metadata, summary, keywords, processing_time):
    try:
        collection = get_collection("pdf_database", "pdf_documents")
        if collection:
            collection.update_one(
                {"document_name": file_metadata['filename']},
                {"$set": {
                    "summary": summary,
                    "keywords": keywords,
                    "status": "processed",
                    "summary_length": len(summary.split()),
                    "keywords_count": len(keywords),
                    "processing_time": processing_time,
                    "timestamp": datetime.now()
                }}
            )
            logging.info(f"Updated document metadata for {file_metadata['filename']}")
    except Exception as e:
        logging.error(f"Error updating document metadata for {file_metadata['filename']}: {e}")

# Function to update document status in case of an error
def update_document_error(file_metadata, error_message):
    try:
        collection = get_collection("pdf_database", "pdf_documents")
        if collection:
            collection.update_one(
                {"document_name": file_metadata['filename']},
                {"$set": {"status": "error", "error_message": error_message, "timestamp": datetime.now()}}
            )
            logging.error(f"Updated document with error for {file_metadata['filename']}: {error_message}")
    except Exception as e:
        logging.error(f"Error updating error status for {file_metadata['filename']}: {e}")

# Function to export MongoDB collection to a JSON file
def export_collection(output_file):
    try:
        collection = get_collection("pdf_database", "pdf_documents")
        if collection:
            documents = collection.find()
            with open(output_file, 'w') as file:
                file.write(bson_json.dumps(list(documents), indent=4))
            logging.info(f"Exported MongoDB collection to {output_file}")
    except Exception as e:
        logging.error(f"Error exporting MongoDB collection: {e}")

# Function to count documents based on their status
def count_documents(status):
    try:
        collection = get_collection("pdf_database", "pdf_documents")
        if collection:
            return collection.count_documents({"status": status})
    except Exception as e:
        logging.error(f"Error counting documents with status {status}: {e}")
    return 0

# Function to check if a document already exists in MongoDB
def document_exists(url):
    try:
        collection = get_collection("pdf_database", "pdf_documents")
        if collection:
            existing_document = collection.find_one({"url": url})
            return existing_document is not None
    except Exception as e:
        logging.error(f"Error checking document existence for URL {url}: {e}")
    return False
