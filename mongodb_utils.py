import os
import pymongo
from datetime import datetime
import bson.json_util as bson_json  # For exporting MongoDB data
import logging
import time

# MongoDB setup with retry for connection failures
def get_mongo_client(uri="mongodb://localhost:27017/"):
    retry_count = 3
    while retry_count > 0:
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.server_info()  # Forces a call to check the server status
            return client
        except pymongo.errors.ConnectionFailure as e:
            retry_count -= 1
            logging.error(f"Failed to connect to MongoDB: {e}. Retrying... ({3 - retry_count}/3)")
            time.sleep(3)
    return None

# Get a specific MongoDB collection
def get_collection(db_name, collection_name):
    client = get_mongo_client()
    if client:
        db = client[db_name]
        return db[collection_name]
    else:
        logging.error(f"Could not connect to MongoDB to get collection: {collection_name}")
    return None




# Function to export MongoDB collection to a JSON file
def export_collection(output_file):
    collection = get_collection("pdf_database", "pdf_documents")
    if collection is not None:  # Corrected from `if collection:`
        try:
            documents = collection.find()
            with open(output_file, 'w') as file:
                file.write(bson_json.dumps(list(documents), indent=4))
            print(f"Exported MongoDB collection to {output_file}")
        except Exception as e:
            logging.error(f"Error exporting MongoDB collection: {e}")
    else:
        logging.error(f"MongoDB collection not found for exporting to {output_file}")

# Other similar cases in `mongodb_utils.py` should be updated as follows:

# Function to insert metadata after downloading a PDF
def insert_metadata(filename, url):
    collection = get_collection("pdf_database", "pdf_documents")
    if collection is not None:  # Corrected from `if collection:`
        metadata = {
            "document_name": filename.split('/')[-1],
            "path": filename,
            "size": os.path.getsize(filename),
            "url": url,
            "status": "downloaded",
            "timestamp": datetime.now(),
            "error_message": None,
            "processing_time": None
        }
        try:
            collection.insert_one(metadata)
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error inserting metadata for {filename}: {e}")

# Function to update MongoDB with processing results
def update_document(filepath, summary, keywords, processing_time):
    collection = get_collection("pdf_database", "pdf_documents")
    if collection is not None:  # Corrected from `if collection:`
        try:
            collection.update_one(
                {"path": filepath},
                {"$set": {
                    "summary": summary,
                    "keywords": keywords,
                    "status": "processed",
                    "summary_length": len(summary.split()),
                    "keywords_count": len(keywords),
                    "processing_time": processing_time,
                    "timestamp": datetime.now(),
                    "error_message": None
                }}
            )
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error updating metadata for {filepath}: {e}")

# Function to update document status in case of an error
def update_document_error(filepath, error_message):
    collection = get_collection("pdf_database", "pdf_documents")
    if collection is not None:  # Corrected from `if collection:`
        try:
            collection.update_one(
                {"path": filepath},
                {"$set": {
                    "status": "error",
                    "error_message": error_message,
                    "timestamp": datetime.now()
                }}
            )
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error updating document error for {filepath}: {e}")

# Function to count documents based on their status
def count_documents(status):
    collection = get_collection("pdf_database", "pdf_documents")
    if collection is not None:  # Corrected from `if collection:`
        try:
            return collection.count_documents({"status": status})
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error counting documents with status {status}: {e}")
    return 0

# Function to check if a document already exists in MongoDB
def document_exists(url):
    collection = get_collection("pdf_database", "pdf_documents")
    if collection is not None:  # Corrected from `if collection:`
        try:
            existing_document = collection.find_one({"url": url})
            return existing_document is not None
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error checking existence of document with URL {url}: {e}")
    return False
