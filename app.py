from flask import Flask, request, render_template, jsonify
import os
import boto3
import logging
import traceback
from pdf_utils import parse_pdf
from summarization import generate_summary, extract_keywords
from mongodb_utils import insert_metadata, update_document, update_document_error
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# AWS S3 setup
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
bucket_name = os.getenv('S3_BUCKET_NAME')

# Logging setup
logging.basicConfig(filename='app_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        if file and file.filename.endswith('.pdf'):
            try:
                start_time = time.time()

                # Upload file to S3
                s3_key = f"uploads/{file.filename}"
                s3_client.upload_fileobj(
                    file, bucket_name, s3_key,
                    ExtraArgs={"ContentType": "application/pdf"}
                )

                # Metadata insertion for the uploaded file
                insert_metadata(s3_key, "Uploaded via Web UI")

                # Start processing the PDF file
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                parsed_text = parse_pdf(s3_object['Body'])

                if parsed_text:
                    # Generate summary and keywords
                    summary = generate_summary(parsed_text)
                    keywords = extract_keywords(parsed_text)
                    processing_time = time.time() - start_time

                    # Update MongoDB with the results
                    update_document(s3_key, summary, keywords, processing_time)

                    # Return the summary and keywords as response
                    return jsonify({"summary": summary, "keywords": keywords}), 200

                else:
                    update_document_error(s3_key, "Failed to parse PDF")
                    return jsonify({"error": "Failed to parse PDF"}), 500

            except Exception as e:
                logging.error(f"Error processing file {file.filename}: {e}")
                logging.error(traceback.format_exc())
                return jsonify({"error": "An error occurred while processing the file"}), 500
        else:
            return jsonify({"error": "Invalid file type. Only PDF files are allowed."}), 400
    else:
        return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.getenv('PORT', 5000)))

