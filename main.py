import os
import json
import shutil
import logging
from pdf_utils import download_pdf, parse_pdf, move_pdf_to_respective_folder, save_parsed_text
from mongodb_utils import document_exists, update_document_error, count_documents, export_collection, get_collection
from summarization import generate_summary, extract_keywords
from json_mongodb_utils import insert_or_update_document_metadata, save_json_to_file

# Logging setup
logging.basicConfig(filename='pipeline_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Define main folder paths
primary_folder = os.path.join(os.path.expanduser("~"), "Desktop", "PDFDownloadParse", "PDFSummary")
short_folder = os.path.join(primary_folder, "short")
medium_folder = os.path.join(primary_folder, "medium")
long_folder = os.path.join(primary_folder, "long")

# Delete existing folders and recreate them
if os.path.exists(primary_folder):
    shutil.rmtree(primary_folder)

# Create necessary folders for PDFs and parsed texts
for folder in [short_folder, medium_folder, long_folder]:
    os.makedirs(os.path.join(folder, "pdfs"), exist_ok=True)
    os.makedirs(os.path.join(folder, "texts"), exist_ok=True)

# Create a dedicated folder for JSON files at the same level as the PDFs
json_folder = os.path.join(primary_folder, "json")
os.makedirs(json_folder, exist_ok=True)

# Load PDF URLs from dataset.json
with open('Dataset.json', 'r') as file:
    dataset = json.load(file)
    pdf_urls = list(dataset.values())

# Clear MongoDB collection before starting
collection = get_collection("pdf_database", "pdf_documents")
collection.delete_many({})
print("Cleared MongoDB collection.")

# Test MongoDB Connection
def test_mongodb_connection():
    collection = get_collection('pdf_database', 'pdf_documents')
    if collection is None:
        print("Failed to connect to MongoDB")
    else:
        print("MongoDB connection is successful")

# Download, move, and parse PDFs
def test_download_move_and_parse_pdfs():
    for url in pdf_urls:
        # Check if the document already exists in MongoDB
        if document_exists(url):
            print(f"Document already processed or downloaded, skipping: {url}")
            continue

        downloaded_file = download_pdf(url, primary_folder)
        if downloaded_file:
            # Move file to the appropriate folder based on its length
            moved_file, text_folder = move_pdf_to_respective_folder(downloaded_file, short_folder, medium_folder, long_folder)
            if moved_file:
                print(f"Moved file to: {moved_file}")
                # Parse the PDF
                text = parse_pdf(moved_file)
                if text:
                    # Save parsed text to a separate folder
                    save_parsed_text(os.path.join(text_folder, os.path.basename(moved_file)), text)
                    print(f"Parsed text saved for: {moved_file}")
                    # Summarize and extract keywords
                    summary = generate_summary(text)
                    keywords = extract_keywords(text)
                    # Save summaries and keywords locally in JSON folder
                    json_filename = os.path.join(json_folder, os.path.basename(moved_file).replace(".pdf", ".json"))

                    json_data = {
                        "summary": summary,
                        "keywords": keywords
                    }

                    with open(json_filename, "w") as json_file:
                        json.dump(json_data, json_file, indent=4)
                    print(f"Saved JSON to: {json_filename}")

                    # Update MongoDB with parsed text summary and keywords
                    insert_or_update_document_metadata(moved_file, summary, keywords)
                    print(f"Parsed and updated MongoDB for: {moved_file}")
                else:
                    update_document_error(moved_file, "Failed to parse PDF.")
                    print(f"Failed to parse PDF: {moved_file}")
            else:
                update_document_error(downloaded_file, "Failed to move file based on length.")
                print(f"Failed to move file: {downloaded_file}")
        else:
            print(f"Failed to download file from URL: {url}")

def test_export_collection():
    # Ensure that `export_collection()` is imported from `pymongo_utils` or `mongodb_utils`
    export_collection("exported_mongodb_collection.json")
    print("Exported MongoDB collection to exported_mongodb_collection.json")

def test_count_documents():
    downloaded_count = count_documents("downloaded")
    processed_count = count_documents("processed")
    error_count = count_documents("error")
    print(f"Downloaded documents: {downloaded_count}")
    print(f"Processed documents: {processed_count}")
    print(f"Error documents: {error_count}")

if __name__ == "__main__":
    # Test MongoDB
    test_mongodb_connection()

    # Test PDF downloading, moving, and parsing
    test_download_move_and_parse_pdfs()

    # Test exporting MongoDB collection
    test_export_collection()

    # Test document count in MongoDB
    test_count_documents()
