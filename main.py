import os
import json
import shutil
from pdf_utils import download_pdf, parse_pdf, move_pdf_to_respective_folder, save_parsed_text
from mongodb_utils import document_exists, update_document_error, count_documents, export_collection
from mongodb_utils import get_collection, insert_metadata, update_document
from summarization import generate_summary, extract_keywords
import logging
import concurrent.futures
import time

# Logging setup
logging.basicConfig(filename='pipeline_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Define main folder paths
primary_folder = os.path.join(os.path.expanduser("~"), "Desktop", "PDFDownloadParse", "PDFSummary")
short_folder = os.path.join(primary_folder, "short")
medium_folder = os.path.join(primary_folder, "medium")
long_folder = os.path.join(primary_folder, "long")
json_folder = os.path.join(primary_folder, ".json")

# Delete existing folders and recreate them
if os.path.exists(primary_folder):
    shutil.rmtree(primary_folder)

# Create necessary folders if they don't exist
for folder in [short_folder, medium_folder, long_folder]:
    os.makedirs(os.path.join(folder, "pdfs"), exist_ok=True)
    os.makedirs(os.path.join(folder, "texts"), exist_ok=True)
os.makedirs(json_folder, exist_ok=True)

# Load PDF URLs from dataset.json
with open('Dataset.json', 'r') as file:
    dataset = json.load(file)
    pdf_urls = list(dataset.values())

# Clear MongoDB collection before starting
collection = get_collection("pdf_database", "pdf_documents")
collection.delete_many({})
print("Cleared MongoDB collection.")

# Download, move, and parse PDFs with concurrency
def process_pdf(url):
    try:
        # Check if the document already exists in MongoDB
        if document_exists(url):
            print(f"Document already processed or downloaded, skipping: {url}")
            return

        # Step 1: Download PDF
        downloaded_file = download_pdf(url, primary_folder)
        if not downloaded_file:
            update_document_error(downloaded_file, "Failed to download file.")
            return

        # Insert metadata after downloading
        insert_metadata(downloaded_file, url)

        # Step 2: Move PDF to appropriate folder based on length
        moved_file, text_folder = move_pdf_to_respective_folder(
            downloaded_file, short_folder, medium_folder, long_folder)
        
        if not moved_file:
            update_document_error(downloaded_file, "Failed to move file based on length.")
            return

        # Step 3: Parse PDF to extract text
        text = parse_pdf(moved_file)
        if not text:
            update_document_error(moved_file, "Failed to parse PDF.")
            return

        # Save parsed text
        save_parsed_text(os.path.join(text_folder, os.path.basename(moved_file)), text)
        print(f"Parsed text saved for: {moved_file}")

        # Step 4: Generate Summary and Keywords
        start_time = time.time()
        summary = generate_summary(text)
        keywords = extract_keywords(text)
        end_time = time.time()

        # Save Summary and Keywords to JSON
        json_data = {
            "document_name": os.path.basename(moved_file),
            "summary": summary,
            "keywords": keywords,
            "processing_time": f"{end_time - start_time:.2f} seconds"
        }
        json_filename = os.path.join(json_folder, os.path.basename(moved_file).replace(".pdf", ".json"))
        with open(json_filename, "w") as json_file:
            json.dump(json_data, json_file, indent=4)
        print(f"Saved JSON to: {json_filename}")

        # Step 5: Clean Up - Delete summaries and keywords folders after saving JSON
        summary_folder = os.path.join(os.path.dirname(moved_file), "summaries")
        keywords_folder = os.path.join(os.path.dirname(moved_file), "keywords")
        
        if os.path.exists(summary_folder):
            shutil.rmtree(summary_folder)
            print(f"Deleted folder: {summary_folder}")

        if os.path.exists(keywords_folder):
            shutil.rmtree(keywords_folder)
            print(f"Deleted folder: {keywords_folder}")

        # Step 6: Update MongoDB
        update_document(moved_file, summary, keywords, end_time - start_time)
        print(f"Parsed and updated MongoDB for: {moved_file}")

    except Exception as e:
        logging.error(f"Error processing PDF {url}: {e}")

# Use concurrent.futures to process PDFs concurrently
def concurrent_pdf_processing(urls):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_pdf, urls)

# Execute the pipeline
if __name__ == "__main__":
    # Test MongoDB Connection
    collection = get_collection('pdf_database', 'pdf_documents')
    if collection is None:
        print("Failed to connect to MongoDB")
    else:
        print("MongoDB connection is successful")

    # Concurrently download, move, parse, summarize, and update MongoDB
    concurrent_pdf_processing(pdf_urls)

    # Exporting MongoDB collection
    export_collection("exported_mongodb_collection.json")
    print("Exported MongoDB collection to exported_mongodb_collection.json")

    # Document counts in MongoDB
    print(f"Downloaded documents: {count_documents('downloaded')}")
    print(f"Processed documents: {count_documents('processed')}")
    print(f"Error documents: {count_documents('error')}")
