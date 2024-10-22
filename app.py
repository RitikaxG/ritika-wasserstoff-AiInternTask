from flask import Flask, request, render_template, jsonify
from flask_cors import CORS
import os
import logging
import json
from pdf_utils import parse_pdf, save_parsed_text
from summarization import generate_summary, extract_keywords
from mongodb_utils import insert_metadata, update_document, update_document_error
from datetime import datetime
import time

app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing

# Logging setup
logging.basicConfig(filename='app_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Route to serve index.html for file upload
@app.route('/')
def index():
    return render_template('index.html')

# Route to handle file upload and process synchronously
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        logging.error("No file part in the request")
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        logging.error("No selected file")
        return jsonify({"error": "No selected file"}), 400

    # Check file size to prevent large uploads (e.g., max 5MB)
    file.seek(0, os.SEEK_END)
    file_length = file.tell()
    if file_length > 5 * 1024 * 1024:  # 5MB limit
        return jsonify({"error": "File size exceeds limit (5MB max)."}), 400
    file.seek(0)  # Reset the file pointer

    if file and file.filename.endswith('.pdf'):
        # Save the uploaded file to a designated folder
        try:
            filepath = os.path.join('uploads', file.filename)
            os.makedirs('uploads', exist_ok=True)
            file.save(filepath)
            logging.info(f"File saved at: {filepath}")
        except Exception as e:
            logging.error(f"Error saving file {file.filename}: {e}")
            return jsonify({"error": "Failed to save file"}), 500

        # Process the file synchronously
        try:
            # Metadata insertion for the uploaded file
            insert_metadata(filepath, "Uploaded via Web UI")

            # Start processing the PDF file
            parsed_text = parse_pdf(filepath)
            if parsed_text:
                # Generate summary and keywords
                start_time = time.time()
                summary = generate_summary(parsed_text)
                keywords = extract_keywords(parsed_text)
                processing_time = time.time() - start_time

                # Save parsed text (optional)
                save_parsed_text(filepath, parsed_text)

                # Save JSON data
                json_folder = os.path.join('json')
                os.makedirs(json_folder, exist_ok=True)
                json_filename = os.path.join(json_folder, os.path.basename(filepath).replace(".pdf", ".json"))
                json_data = {
                    "summary": summary,
                    "keywords": keywords,
                    "processing_time": processing_time,
                    "timestamp": str(datetime.now())
                }
                with open(json_filename, "w") as json_file:
                    json.dump(json_data, json_file, indent=4)
                logging.info(f"Saved JSON to: {json_filename}")

                # Update MongoDB with the results
                update_document(filepath, summary, keywords, processing_time)

                # Return the summary and keywords as response
                return jsonify({"summary": summary, "keywords": keywords}), 200

            else:
                update_document_error(filepath, "Failed to parse PDF")
                logging.error(f"Failed to parse PDF: {filepath}")
                return jsonify({"error": "Failed to parse PDF"}), 500

        except Exception as e:
            logging.error(f"Error processing file {filepath}: {e}")
            update_document_error(filepath, str(e))
            return jsonify({"error": f"An error occurred while processing the file: {str(e)}"}), 500

    else:
        logging.error("Invalid file type")
        return jsonify({"error": "Invalid file type. Only PDF files are allowed."}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
