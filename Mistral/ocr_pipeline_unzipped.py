#!/usr/bin/env python3
import os
import io
import csv
import json
import time
import hashlib
import traceback
import re
from pathlib import PurePosixPath
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional
from dotenv import load_dotenv

import boto3
from mistralai import DocumentURLChunk, FileTypedDict, Mistral


load_dotenv()
API_KEY = os.getenv("MISTRAL_API_KEY")
if not API_KEY:
    raise RuntimeError("MISTRAL_API_KEY not found in .env")

# INPUT: already-unzipped PDFs by year-like subfolders
S3_BUCKET      = "fed-data-storage"
INPUT_ROOT     = "Unziped_Documents/"           
# OUTPUTS: keep separate from legacy pipeline
OUT_JSON_ROOT  = "CapIQMistral_Updated/"         # OCR JSON (grouped by YEAR we infer)
OUT_CSV_ROOT   = "ProcessedMistralUpdated/"      # CSVs + markers (NEW namespace)
MARKER_PREFIX  = f"{OUT_CSV_ROOT}processed_markers/"

PROCESSED_CSV_KEY = f"{OUT_CSV_ROOT}processed_files_CapIQ.csv"
FAILED_CSV_KEY    = f"{OUT_CSV_ROOT}failed_files_CapIQ.csv"

MISTRAL_MODEL    = "mistral-ocr-latest"
MAX_OCR_RETRIES  = 3
RETRY_BACKOFF    = 5  # seconds (linear backoff: n * RETRY_BACKOFF)

# Clients
s3 = boto3.client("s3")
mistral = Mistral(api_key=API_KEY)


# =========================
# HELPERS
# =========================
def safe_basename(key: str) -> str:
    """Return last path component of an S3 key."""
    p = PurePosixPath(key.replace("\\", "/"))
    return p.name

def split_stem_ext(name: str) -> Tuple[str, str]:
    """Lowercased extension, original stem."""
    if "." in name:
        stem, ext = name.rsplit(".", 1)
        return stem, f".{ext.lower()}"
    return name, ""

def sanitize_for_s3(name: str) -> str:
    safe = "".join(c if 32 <= ord(c) < 127 else "_" for c in name)[:200]
    return safe or "document"

def marker_key_for_pdf(pdf_key: str) -> str:
    # Unique per source object key (no ZIP context in new pipeline)
    h = hashlib.sha1(f"{pdf_key}".encode("utf-8")).hexdigest()
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

def append_row_to_csv_s3(row: List[str], csv_key: str, header: List[str]):
    """
    Downloads CSV if exists, appends a row, re-uploads.
    """
    rows: List[List[str]] = []
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=csv_key)
        body = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.reader(body)
        rows = list(reader)
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise

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

def list_top_level_dirs(prefix_root: str) -> List[str]:
    """
    Return top-level 'directories' (common prefixes) under prefix_root.
    Example return items: 'Unziped_Documents/2001/', 'Unziped_Documents/2001_Q4/', ...
    """
    dirs = []
    token = None
    while True:
        kwargs = dict(Bucket=S3_BUCKET, Prefix=prefix_root, Delimiter="/")
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for cp in resp.get("CommonPrefixes", []):
            dirs.append(cp["Prefix"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return dirs

def list_pdfs_recursively(prefix: str) -> List[str]:
    """
    Recursively list .pdf/.PDF keys under prefix.
    """
    keys = []
    token = None
    while True:
        kwargs = dict(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1000)
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".pdf"):
                keys.append(key)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def extract_folder_year_guess(dir_prefix: str) -> Optional[int]:
    """
    dir_prefix like 'Unziped_Documents/2001_Q4/' -> try to extract a leading YYYY.
    """
    folder = dir_prefix[len(INPUT_ROOT):].strip("/")
    m = re.match(r"(19|20)\d{2}", folder)
    if m:
        return int(m.group(0))
    return None

def extract_year_from_filename(stem: str, folder_year_fallback: Optional[int]) -> str:
    """
    Year selection priority:
      1) Full ISO date (YYYY[-_]MM[-_]DD) -> take MAX year
      2) Partial ISO (YYYY[-_]MM)         -> MAX year
      3) Plain 4-digit years               -> MAX year
      4) Fallback to folder-year
      5) 'unknown'
    Also clamps to 1900..(current_year+1)
    """
    current_year = time.gmtime().tm_year
    min_y, max_y = 1900, current_year + 1

    years = []

    # 1) Full ISO date
    for m in re.finditer(r"(19|20)\d{2}[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12]\d|3[01])", stem):
        years.append(int(m.group(0)[:4]))
    if years:
        y = max(years)
        return str(max(min(y, max_y), min_y))

    # 2) Partial ISO YYYY-MM
    years = []
    for m in re.finditer(r"(19|20)\d{2}[-_](0[1-9]|1[0-2])", stem):
        years.append(int(m.group(0)[:4]))
    if years:
        y = max(years)
        return str(max(min(y, max_y), min_y))

    # 3) Plain 4-digit years (word/separator bounded)
    plain = re.findall(r"\b(19|20)\d{2}\b", stem)
    if plain:
        # 'plain' contains only the first 2 digits group; re-find actual numbers robustly:
        plain_all = re.findall(r"\b((?:19|20)\d{2})\b", stem)
        if plain_all:
            y = max(int(v) for v in plain_all)
            y = max(min(y, max_y), min_y)
            return str(y)

    # 4) Fallbacks
    if folder_year_fallback and (min_y <= folder_year_fallback <= max_y):
        return str(folder_year_fallback)

    return "unknown"

def mistral_ocr_from_bytes(pdf_bytes: bytes, display_name: str) -> dict:
    last_err = None
    for attempt in range(1, MAX_OCR_RETRIES + 1):
        try:
            file_dict: FileTypedDict = {
                "file_name": f"{display_name}.pdf",
                "content": pdf_bytes,
            }
            uploaded = mistral.files.upload(file=file_dict, purpose="ocr")
            signed   = mistral.files.get_signed_url(file_id=uploaded.id, expiry=200)
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
    # exhausted
    raise last_err

def upload_json_to_s3(payload: dict, key: str):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"📤 Uploaded JSON → s3://{S3_BUCKET}/{key}")


# =========================
# MAIN
# =========================
def main():
    import argparse
    parser = argparse.ArgumentParser(description="OCR pipeline for already-unzipped PDF years (S3).")
    parser.add_argument("--years", nargs="+", default=[],
                        help="Year prefixes to match under Unziped_Documents/ (e.g. 2001 2011 2022_Q4).")
    parser.add_argument("--year-list-file", default=None,
                        help="Optional file with whitespace-separated year prefixes; used if --years empty.")
    parser.add_argument("--job-index", type=int, default=None,
                        help="Optional 1-based job index (e.g., LSF array index). Used with --year-list-file to pick one token.")
    parser.add_argument("--ocr-concurrency", type=int, default=1,
                        help="Optional per-job concurrency for PDFs (1-4 recommended).")
    #parser.add_argument("--max-ocr-retries", type=int, default=MAX_OCR_RETRIES)
    #parser.add_argument("--retry-backoff", type=int, default=RETRY_BACKOFF)
    args = parser.parse_args()

    # allow runtime override
    #MAX_OCR_RETRIES = args.max_ocr_retries
    #RETRY_BACKOFF   = args.retry_backoff

    # resolve years list:
    years: List[str] = list(args.years)
    if not years and args.year_list_file:
        # load tokens from file
        with open(args.year_list_file, "r", encoding="utf-8") as f:
            tokens = f.read().split()
        if args.job_index:  # one token per job index (1-based)
            idx = args.job_index - 1
            if 0 <= idx < len(tokens):
                years = [tokens[idx]]
            else:
                print(f"ℹ️ job-index {args.job_index} exceeds year-list size ({len(tokens)}). Nothing to do.")
                return
        else:
            # no job-index: use all tokens
            years = tokens

    # Fallback to LSF env if provided
    if not years and args.year_list_file:
        lsf_idx = os.getenv("LSB_JOBINDEX")
        if lsf_idx and lsf_idx.isdigit():
            with open(args.year_list_file, "r", encoding="utf-8") as f:
                tokens = f.read().split()
            idx = int(lsf_idx) - 1
            if 0 <= idx < len(tokens):
                years = [tokens[idx]]

    if not years:
        raise SystemExit("No years provided. Use --years ... or --year-list-file with --job-index/LSB_JOBINDEX.")

    print(f" matching top-level subfolders under s3://{S3_BUCKET}/{INPUT_ROOT} for prefixes: {years}")

    # Find matching year directories
    all_dirs = list_top_level_dirs(INPUT_ROOT)
    match_dirs = []
    for d in all_dirs:
        folder = d[len(INPUT_ROOT):].strip("/")
        if any(folder.startswith(y) for y in years):
            match_dirs.append(d)

    print(f"Found {len(match_dirs)} matching subfolders.")
    for d in match_dirs:
        print(f"   - {d}")

    # Gather PDF keys
    pdf_keys: List[str] = []
    for d in match_dirs:
        pdfs = list_pdfs_recursively(d)
        print(f"   → {d} : {len(pdfs)} PDFs")
        pdf_keys.extend(pdfs)

    print(f"🧾 Total PDFs to consider: {len(pdf_keys)}")

    total_ok = 0
    total_fail = 0
    total_skipped = 0

    def process_one(pdf_key: str) -> Tuple[str, str]:
        # Returns ("ok"/"skip"/"fail", pdf_key)
        mkey = marker_key_for_pdf(pdf_key)
        try:
            #if marker_exists(S3_BUCKET, mkey):
            #    return ("skip", pdf_key)

            # Download
            obj = s3.get_object(Bucket=S3_BUCKET, Key=pdf_key)
            pdf_bytes = obj["Body"].read()

            base = safe_basename(pdf_key)
            stem, ext = split_stem_ext(base)

            # Determine year: prefer YYYY-MM-DD in filename, else folder prefix, else unknown
            folder_guess = extract_folder_year_guess("/".join(pdf_key.split("/")[:2]) + "/")  # best-effort
            year = extract_year_from_filename(stem, folder_guess)

            ocr_dict = mistral_ocr_from_bytes(pdf_bytes, display_name=stem)

            payload = {
                "source": {"pdf_key": pdf_key},
                "ocr_output": ocr_dict,
            }

            safe_stem = sanitize_for_s3(stem)
            json_key = f"{OUT_JSON_ROOT}{year}/{safe_stem}.json"

            upload_json_to_s3(payload, json_key)

            write_marker(S3_BUCKET, mkey, {
                "pdf_key": pdf_key,
                "json_key": json_key,
                "ts": int(time.time())
            })

            append_row_to_csv_s3(
                row=[pdf_key, year, json_key],
                csv_key=PROCESSED_CSV_KEY,
                header=["pdf_key", "year", "json_key"]
            )

            return ("ok", pdf_key)

        except Exception as e:
            err = f"{e.__class__.__name__}: {e}"
            traceback.print_exc()
            append_row_to_csv_s3(
                row=[pdf_key, err],
                csv_key=FAILED_CSV_KEY,
                header=["pdf_key", "error_message"]
            )
            return ("fail", pdf_key)

    # run (optionally threaded)
    if args.ocr_concurrency > 1:
        with ThreadPoolExecutor(max_workers=args.ocr_concurrency) as ex:
            futures = {ex.submit(process_one, k): k for k in pdf_keys}
            for fut in as_completed(futures):
                status, _ = fut.result()
                if status == "ok":
                    total_ok += 1
                elif status == "skip":
                    total_skipped += 1
                else:
                    total_fail += 1
    else:
        for k in pdf_keys:
            status, _ = process_one(k)
            if status == "ok":
                total_ok += 1
            elif status == "skip":
                total_skipped += 1
            else:
                total_fail += 1

    print(f"\n Done. Processed: {total_ok} | Skipped: {total_skipped} | Failed: {total_fail}")
    print(f"OCR JSONs: s3://{S3_BUCKET}/{OUT_JSON_ROOT}")
    #print(f"Markers:   s3://{S3_BUCKET}/{MARKER_PREFIX}")
    print(f"CSVs:      s3://{S3_BUCKET}/{OUT_CSV_ROOT}")


if __name__ == "__main__":
    main()
