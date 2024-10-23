import json
import pymongo
import logging
from datetime import datetime
from bson import json_util as bson_json  # Corrected import for json_util

# MongoDB connection setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["pdf_database"]
collection = db["pdf_documents"]

# Function to insert/update document metadata with summaries and keywords in MongoDB
def insert_or_update_document_metadata(filepath, summary, keywords):
    try:
        json_data = create_json_structure(summary, keywords)
        
        # Update MongoDB with the generated JSON structure
        collection.update_one(
            {"path": filepath},
            {"$set": {
                "summary": json_data["summary"],
                "keywords": json_data["keywords"],
                "status": "processed",
                "last_updated": datetime.now()
            }},
            upsert=True
        )
        print(f"Updated metadata in MongoDB for: {filepath}")
    except Exception as e:
        logging.error(f"Error updating MongoDB for {filepath}: {e}")

# Function to create JSON structure for summaries and keywords
def create_json_structure(summary, keywords):
    return {
        "summary": summary,
        "keywords": keywords
    }

# Function to save summaries and keywords in a local JSON file
def save_json_to_file(filepath, summary, keywords):
    try:
        json_filename = filepath.replace(".pdf", "_metadata.json")
        json_data = create_json_structure(summary, keywords)
        
        with open(json_filename, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        print(f"Saved JSON metadata to: {json_filename}")
    except Exception as e:
        logging.error(f"Error saving JSON metadata for {filepath}: {e}")

# Function to handle error logging for JSON/MongoDB operations
def handle_error(filepath, error_message):
    try:
        collection.update_one(
            {"path": filepath},
            {"$set": {
                "status": "error",
                "error_message": error_message,
                "last_updated": datetime.now()
            }}
        )
        logging.error(f"Error logged in MongoDB for {filepath}: {error_message}")
    except Exception as e:
        logging.error(f"Failed to log error in MongoDB for {filepath}: {e}")

