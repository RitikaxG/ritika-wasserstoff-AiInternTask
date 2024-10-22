from flask import Flask, request, render_template, jsonify
import os
import logging
import json
from pdf_utils import parse_pdf
from summarization import generate_summary, extract_keywords
from mongodb_utils import insert_metadata, update_document, update_document_error
from datetime import datetime
import time

app = Flask(__name__)

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
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file and file.filename.endswith('.pdf'):
        try:
            start_time = time.time()

            # Metadata insertion for the uploaded file
            insert_metadata(file.filename, "Uploaded via Web UI")

            # Start processing the PDF file (read the file directly without saving locally)
            parsed_text = parse_pdf(file)
            if parsed_text:
                # Generate summary and keywords
                summary = generate_summary(parsed_text)
                keywords = extract_keywords(parsed_text)
                processing_time = time.time() - start_time

                # Prepare JSON data for response and MongoDB
                json_data = {
                    "summary": summary,
                    "keywords": keywords,
                    "processing_time": processing_time,
                    "timestamp": str(datetime.now())
                }

                # Update MongoDB with the results
                update_document(file.filename, summary, keywords, processing_time)

                # Return the summary and keywords as response
                return jsonify(json_data), 200

            else:
                update_document_error(file.filename, "Failed to parse PDF")
                return jsonify({"error": "Failed to parse PDF"}), 500

        except Exception as e:
            logging.error(f"Error processing file {file.filename}: {e}", exc_info=True)
            return jsonify({"error": f"An error occurred while processing the file: {str(e)}"}), 500

    else:
        return jsonify({"error": "Invalid file type. Only PDF files are allowed."}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
