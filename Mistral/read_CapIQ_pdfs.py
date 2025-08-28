import os
import io
import csv
import json
import time
import zipfile
import hashlib
import traceback
from pathlib import PurePosixPath
from typing import List, Tuple
import re


import boto3
from dotenv import load_dotenv
from mistralai import DocumentURLChunk, FileTypedDict, Mistral


# =========================
# CONFIG
# =========================
load_dotenv()
API_KEY = os.getenv("MISTRAL_API_KEY")
if not API_KEY:
    raise RuntimeError("MISTRAL_API_KEY not found in .env")


S3_BUCKET     = "fed-data-storage"
ZIP_PREFIX    = "Updated_Documents/"            # input ZIPs
JSON_PREFIX   = "MistralCapIQUpdated/"          # OCR JSON outputs
CSV_PREFIX    = "ProcessedMistral/"             # master CSVs + markers
MARKER_PREFIX = f"{CSV_PREFIX}processed_markers/"


MISTRAL_MODEL    = "mistral-ocr-latest"
MAX_OCR_RETRIES  = 3
RETRY_BACKOFF    = 5  # seconds (linear backoff: n * RETRY_BACKOFF)


PROCESSED_CSV_KEY = f"{CSV_PREFIX}processed_files_CapIQ.csv"
FAILED_CSV_KEY    = f"{CSV_PREFIX}failed_files_CapIQ.csv"


# =========================
# CLIENTS
# =========================
s3 = boto3.client("s3")
mistral = Mistral(api_key=API_KEY)


# =========================
# HELPERS
# =========================
def list_zip_keys(bucket: str, prefix: str) -> List[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".zip"):
                keys.append(key)
    return keys


def split_zip_member(member_name: str) -> Tuple[str, str]:
    p = PurePosixPath(member_name.replace("\\", "/"))
    name = p.name
    if "." in name:
        stem, ext = name.rsplit(".", 1)
        return stem, f".{ext.lower()}"
    return name, ""


def sanitize_for_s3(name: str) -> str:
    safe = "".join(c if 32 <= ord(c) < 127 else "_" for c in name)[:200]
    return safe or "document"


def marker_key_for_pdf(zip_key: str, zip_member: str) -> str:
    h = hashlib.sha1(f"{zip_key}||{zip_member}".encode("utf-8")).hexdigest()
    return f"{MARKER_PREFIX}{h}.ok"


def marker_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def write_marker(bucket: str, key: str, payload: dict):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def mistral_ocr_from_bytes(pdf_bytes: bytes, display_name: str) -> dict:
    last_err = None
    for attempt in range(1, MAX_OCR_RETRIES + 1):
        try:
            file_dict: FileTypedDict = {
                "file_name": f"{display_name}.pdf",
                "content": pdf_bytes,
            }
            uploaded = mistral.files.upload(file=file_dict, purpose="ocr")
            signed   = mistral.files.get_signed_url(file_id=uploaded.id, expiry=200
            )


            resp = mistral.ocr.process(
                document=DocumentURLChunk(document_url=signed.url),
                model=MISTRAL_MODEL,
                include_image_base64=True,
            )
            return json.loads(resp.model_dump_json())


        except Exception as e:
            last_err = e
            if attempt < MAX_OCR_RETRIES:
                wait_s = attempt * RETRY_BACKOFF
                print(f"⚠️ OCR failed (attempt {attempt}/{MAX_OCR_RETRIES}); retrying in {wait_s}s — {e}")
                time.sleep(wait_s)
    raise last_err


def upload_json_to_s3(payload: dict, key: str):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"📤 Uploaded JSON → s3://{S3_BUCKET}/{key}")


def append_row_to_csv_s3(row: List[str], csv_key: str, header: List[str]):
    """
    Downloads CSV if exists, appends a row, re-uploads.
    """
    import io
    rows = []
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=csv_key)
        body = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.reader(body)
        rows = list(reader)
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise


    # If empty, write header
    if not rows:
        rows = [header]


    rows.append(row)


    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(rows)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=csv_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"📤 Appended row to s3://{S3_BUCKET}/{csv_key}")


# =========================
# MAIN
# =========================
def main():
    zip_keys = list_zip_keys(S3_BUCKET, ZIP_PREFIX)
    print(f"Found {len(zip_keys)} ZIP files under s3://{S3_BUCKET}/{ZIP_PREFIX}")


    total_ok, total_fail, total_skipped = 0, 0, 0


    for zip_key in zip_keys:
        print(f"\n🔍 ZIP: {zip_key}")


        # Fetch ZIP to memory
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=zip_key)
            zip_blob = obj["Body"].read()
        except Exception as e:
            err = f"read-zip-failed: {e}"
            print(f"❌ {err}")
            traceback.print_exc()
            append_row_to_csv_s3(["", zip_key, err], FAILED_CSV_KEY,
                                 header=["pdf_identifier", "zip_file", "error_message"])
            total_fail += 1
            continue


        try:
            with zipfile.ZipFile(io.BytesIO(zip_blob)) as zf:
                for info in zf.infolist():
                    stem, ext = split_zip_member(info.filename)
                    if ext != ".pdf":
                        continue


                    pdf_identifier = info.filename.replace("\\", "/")
                    mkey = marker_key_for_pdf(zip_key, pdf_identifier)


                    if marker_exists(S3_BUCKET, mkey):
                        print(f"  ⏭️ Skipping already processed: {pdf_identifier}")
                        total_skipped += 1
                        continue


                    print(f"  📄 PDF: {pdf_identifier}")
                    try:
                        pdf_bytes = zf.read(info)


                        ocr_dict = mistral_ocr_from_bytes(pdf_bytes, display_name=stem)


                        payload = {
                            "source": {
                                "zip_key": zip_key,
                                "zip_member": info.filename,
                                "pdf_identifier": pdf_identifier,
                            },
                            "ocr_output": ocr_dict,
                        }


                        safe_stem = sanitize_for_s3(stem)
                # Extract the 4-digit year from the stem        
                        match = re.search(r'(19|20)\d{2}', stem)
                        year = match.group(0) if match else "unknown"
                    # Put JSON under MistralCapIQUpdated/<year>/<filename>.json
                        json_key = f"{JSON_PREFIX}{year}/{safe_stem}.json"
                        upload_json_to_s3(payload, json_key)


                        # Mark success
                        write_marker(S3_BUCKET, mkey, {
                            "zip_key": zip_key,
                            "zip_member": info.filename,
                            "json_key": json_key,
                            "ts": int(time.time())
                        })
                        append_row_to_csv_s3([pdf_identifier, zip_key], PROCESSED_CSV_KEY,
                                             header=["pdf_identifier", "zip_file"])
                        total_ok += 1


                    except Exception as e:
                        err = f"{e.__class__.__name__}: {e}"
                        print(f"  ❌ Failed PDF: {pdf_identifier} — {err}")
                        traceback.print_exc()
                        append_row_to_csv_s3([pdf_identifier, zip_key, err], FAILED_CSV_KEY,
                                             header=["pdf_identifier", "zip_file", "error_message"])
                        total_fail += 1


        except Exception as e:
            err = f"{e.__class__.__name__}: {e}"
            print(f"❌ ZIP open error: {zip_key} — {err}")
            traceback.print_exc()
            append_row_to_csv_s3(["", zip_key, err], FAILED_CSV_KEY,
                                 header=["pdf_identifier", "zip_file", "error_message"])
            total_fail += 1


    print(f"\n✅ Done. Processed: {total_ok} | Skipped: {total_skipped} | Failed: {total_fail}")
    print(f"OCR JSONs: s3://{S3_BUCKET}/{JSON_PREFIX}")
    print(f"Markers:   s3://{S3_BUCKET}/{MARKER_PREFIX}")
    print(f"CSVs:      s3://{S3_BUCKET}/{CSV_PREFIX}")


if __name__ == "__main__":
    main()