import requests
import fitz  # PyMuPDF
import logging
from hashlib import sha256
import time
from retry import retry
from PyPDF2 import PdfReader

# Logging setup
logging.basicConfig(filename='pdf_utils.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to download a PDF with retry mechanism
@retry(tries=3, delay=5, backoff=2)
def download_pdf(url):
    try:
        response = requests.get(url, stream=True, timeout=10, verify=False)
        if response.status_code == 200:
            file_content = response.content
            file_name = sha256(url.encode()).hexdigest() + ".pdf"
            logging.info(f"Downloaded: {file_name}")
            return file_name, file_content
        else:
            logging.error(f"Failed to download {url} with status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")
        raise
    return None, None

# Function to parse a PDF from a file-like object
def parse_pdf(file_like_object):
    try:
        pdf_reader = PdfReader(file_like_object)
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text()
        return text
    except Exception as e:
        logging.error(f"Error parsing PDF: {e}")
        return None

# Function to save parsed text to a file-like storage (optional: for debugging or further use)
def save_parsed_text(file_like_name, parsed_text):
    try:
        txt_file_name = file_like_name.replace('.pdf', '.txt')
        with open(txt_file_name, 'w') as txt_file:
            txt_file.write(parsed_text)
        logging.info(f"Saved parsed text to {txt_file_name}")
    except Exception as e:
        logging.error(f"Error saving parsed text: {e}")

# Function to determine the number of pages in a PDF file-like object
def determine_pdf_page_count(file_like_object):
    try:
        document = fitz.open(stream=file_like_object, filetype="pdf")
        page_count = document.page_count
        document.close()
        return page_count
    except Exception as e:
        logging.error(f"Error determining page count: {e}")
        return None

# Function to categorize a PDF based on page count
def categorize_pdf(file_like_object):
    try:
        page_count = determine_pdf_page_count(file_like_object)
        
        if page_count is None:
            return "unknown"

        if 1 <= page_count <= 10:
            return "short"
        elif 11 <= page_count <= 30:
            return "medium"
        else:
            return "long"
    except Exception as e:
        logging.error(f"Error categorizing PDF: {e}")
        return "unknown"

# Function to handle the entire PDF processing pipeline
def process_pdf(file_like_object):
    try:
        # Extract the text content from the PDF
        parsed_text = parse_pdf(file_like_object)

        if not parsed_text:
            logging.error("Parsed text is empty or None")
            return None

        # Determine PDF category based on the page count
        pdf_category = categorize_pdf(file_like_object)

        logging.info(f"PDF processed. Category: {pdf_category}")
        return parsed_text, pdf_category

    except Exception as e:
        logging.error(f"Error in processing PDF: {e}")
        return None, None

