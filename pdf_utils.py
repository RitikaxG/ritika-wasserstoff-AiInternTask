import os
import requests
import fitz  # PyMuPDF
import logging
from hashlib import sha256
import time
from retry import retry

# Logging setup
logging.basicConfig(filename='pdf_utils.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to download a PDF with retry mechanism
@retry(tries=3, delay=5, backoff=2)
def download_pdf(url, folder):
    try:
        response = requests.get(url, stream=True, timeout=10, verify=False)
        if response.status_code == 200:
            filename = os.path.join(folder, sha256(url.encode()).hexdigest() + ".pdf")
            with open(filename, "wb") as pdf_file:
                for chunk in response.iter_content(chunk_size=1024):
                    pdf_file.write(chunk)
            logging.info(f"Downloaded: {filename}")
            return filename
        else:
            logging.error(f"Failed to download {url} with status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")
        raise
    return None

# Function to parse a PDF and extract text
def parse_pdf(filepath):
    try:
        start_time = time.time()
        document = fitz.open(filepath)
        text = ""
        for page_num in range(document.page_count):
            page = document.load_page(page_num)
            text += page.get_text()
        document.close()
        end_time = time.time()
        logging.info(f"Parsed {filepath} in {end_time - start_time:.2f} seconds.")
        return text
    except Exception as e:
        logging.error(f"Error parsing {filepath}: {e}")
    return None

# Function to move a PDF to a respective folder based on its page count
def move_pdf_to_respective_folder(filepath, short_folder, medium_folder, long_folder):
    try:
        document = fitz.open(filepath)
        page_count = document.page_count
        document.close()

        # Determine the appropriate folder
        if 1 <= page_count <= 10:
            destination_folder = os.path.join(short_folder, "pdfs")
            parsed_text_folder = os.path.join(short_folder, "texts")
        elif 11 <= page_count <= 30:
            destination_folder = os.path.join(medium_folder, "pdfs")
            parsed_text_folder = os.path.join(medium_folder, "texts")
        else:
            destination_folder = os.path.join(long_folder, "pdfs")
            parsed_text_folder = os.path.join(long_folder, "texts")

        # Ensure that the destination and parsed text folders exist
        os.makedirs(destination_folder, exist_ok=True)
        os.makedirs(parsed_text_folder, exist_ok=True)

        # Move the file to the respective folder
        new_filepath = os.path.join(destination_folder, os.path.basename(filepath))
        os.rename(filepath, new_filepath)
        logging.info(f"Moved file {filepath} to {new_filepath}")
        return new_filepath, parsed_text_folder
    except Exception as e:
        logging.error(f"Error moving file {filepath}: {e}")
    return None, None

# Function to save parsed text to a .txt file
def save_parsed_text(filepath, text):
    text_folder = os.path.dirname(filepath).replace("pdfs", "texts")
    os.makedirs(text_folder, exist_ok=True)
    text_filename = os.path.join(text_folder, os.path.basename(filepath).replace(".pdf", ".txt"))
    try:
        with open(text_filename, "w") as text_file:
            text_file.write(text)
        logging.info(f"Saved parsed text to: {text_filename}")
    except Exception as e:
        logging.error(f"Error saving parsed text for {filepath}: {e}")
