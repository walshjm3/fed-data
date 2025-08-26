import os

import boto3
import zipfile
import io
import csv
from pathlib import Path
from mistralai import DocumentURLChunk, FileTypedDict, Mistral
import json
import traceback
from dotenv import load_dotenv
load_dotenv()


# Load environment variables from .env file
api_key = os.getenv("MISTRAL_API_KEY")

client = Mistral(api_key=api_key)

# Initialize the S3 client
s3 = boto3.client("s3")

bucket_name = "fed-data-storage"
prefix = "Updated_Documents/"

PROCESSED_FILE = "processed_files.csv"
FAILED_FILE = "failed_files.csv"

def load_processed_files():
    if not Path(PROCESSED_FILE).exists():
        return set()
    with open(PROCESSED_FILE, newline="") as f:
        return set(row[0] for row in csv.reader(f))

def mark_file_as_processed(pdf_name: str):
    with open(PROCESSED_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([pdf_name])

def log_failure(pdf_name: str, zip_name: str, error_msg: str):
    header_needed = not Path(FAILED_FILE).exists()
    with open(FAILED_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if header_needed:
            writer.writerow(["pdf_name", "zip_file", "error_message"])
        writer.writerow([pdf_name, zip_name, error_msg])

def list_zip_files(bucket, prefix):
    zip_keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".zip"):
                zip_keys.append(key)
    return zip_keys

def read_pdf(pdf_bytes: bytes, name: str):
    file_dict: FileTypedDict = {
        "file_name": f"{name}.pdf",
        "content": pdf_bytes
    }

    uploaded_file = client.files.upload(
        file=file_dict,
        purpose="ocr"
    )

    signed_url = client.files.get_signed_url(file_id=uploaded_file.id, expiry=1)

    pdf_response = client.ocr.process(
        document=DocumentURLChunk(document_url=signed_url.url),
        model="mistral-ocr-latest",
        include_image_base64=True
    )

    response_dict = json.loads(pdf_response.model_dump_json())

    output_file = Path(f"./MistralCapIQUpdated/{name}.json")
    output_file.write_text(json.dumps(response_dict, indent=2))
    s3.upload_file(str(output_file), "fed-data-storage", f"MistralCapIQUpdated/{output_file.name}")
    output_file.unlink()  # remove local JSON file

def main():
    processed_files = load_processed_files()
    zip_files = list_zip_files(bucket_name, prefix)
    print(f"Found {len(zip_files)} ZIP files.")

    for key in zip_files:
        print(f"\n🔍 Processing ZIP file: {key}")
        try:
            response = s3.get_object(Bucket=bucket_name, Key=key)
            zip_content = response["Body"].read()

            with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
                for file_info in z.infolist():
                    filename = file_info.filename
                    if filename.lower().endswith(".pdf"):
                        pdf_name = filename.split("\\")[-1].replace(".pdf", "")
                        if pdf_name in processed_files:
                            print(f"  ⏭️ Skipping (already processed): {pdf_name}")
                            continue

                        print(f"  📄 Processing PDF: {pdf_name}")
                        try:
                            pdf_bytes = z.read(file_info)
                            read_pdf(pdf_bytes, pdf_name)
                            mark_file_as_processed(pdf_name)
                        except Exception as e:
                            error_msg = f"{e}"
                            print(f"  ❌ Failed to read/process PDF '{pdf_name}': {error_msg}")
                            traceback.print_exc()
                            log_failure(pdf_name, key, error_msg)
        except Exception as e:
            error_msg = f"{e}"
            print(f"❌ Error reading ZIP '{key}': {error_msg}")
            traceback.print_exc()
            log_failure("", key, error_msg)

if __name__ == "__main__":
    main()
