import os
import boto3
import json
import re
import pandas as pd
from pathlib import Path
import csv
from mistralai.models import OCRResponse
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# === CONFIG ===
bucket_name = "fed-data-storage"
# Only process JSON inside this folder:
prefix = "MistralCapIQUpdated/2023/"
# Limit how many files to process per run
MAX_FILES = 20

insiders_dir = Path("csv_testing/insiders")
securities_dir = Path("csv_testing/securities")
tracking_csv = Path("gemini_results_third.csv")

genai.configure(api_key=os.getenv("GENAI_API_KEY"))

# === DIR SETUP ===
insiders_dir.mkdir(parents=True, exist_ok=True)
securities_dir.mkdir(parents=True, exist_ok=True)

# === S3 CLIENT ===
s3 = boto3.client("s3")


def list_all_s3_objects(bucket: str, prefix: str) -> list:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_objects = []
    for page in pages:
        contents = page.get("Contents", [])
        all_objects.extend(contents)
    return all_objects


# === TRACKING ===
def load_tracked_files():
    if not tracking_csv.exists():
        return {}
    with open(tracking_csv, newline="") as f:
        return {row[0]: row[1] for row in csv.reader(f)}


def update_tracking(file: str, status: str, error: str = "", bank_name: str = "", year: str = "", presence: str = ""):
    header_needed = not tracking_csv.exists()
    with open(tracking_csv, "a", newline="") as f:
        writer = csv.writer(f)
        if header_needed:
            # FIX: include 'presence' in header since we write it below
            writer.writerow(["file", "status", "error", "bank_name", "year", "presence"])
        writer.writerow([file, status, error, bank_name, year, presence])


# === HELPER FUNCTIONS ===
def extract_bank_name(markdown: str, filename: str) -> str:
    match = re.search(r"(?i)(?:Legal Title of Holding Company|Reporter's Name.*?)\n+([A-Z0-9 .,&'’\-]+)", markdown)
    if match:
        return match.group(1).strip()
    file_match = re.search(r"([^/\\]+)_Y-6_\d{4}-\d{2}-\d{2}_English", filename)
    return file_match.group(1).replace("_", " ").strip() if file_match else ""


def extract_fiscal_year(markdown: str, filename: str) -> str:
    match = re.search(r"Date of Report.*?:\s*(?:\$)?\s*(\d{2})\s*/\s*(\d{2})\s*/\s*(\d{4})", markdown, re.IGNORECASE) or \
            re.search(r"fiscal year.*?(\d{4})", markdown, re.IGNORECASE)
    if match:
        return match.group(3) if len(match.groups()) == 3 else match.group(1)
    file_match = re.search(r"_Y-6_(\d{4})-\d{2}-\d{2}_English", filename)
    return file_match.group(1) if file_match else ""


def replace_images_in_markdown(markdown_str: str, images_dict: dict) -> str:
    for img_name, base64_str in images_dict.items():
        markdown_str = markdown_str.replace(f"![{img_name}]({img_name})", f"![{img_name}]({base64_str})")
    return markdown_str


def get_combined_markdown(ocr_response: OCRResponse) -> str:
    markdowns = []
    for page in ocr_response.pages:
        image_data = {img.id: img.image_base64 for img in page.images}
        markdowns.append(replace_images_in_markdown(page.markdown, image_data))
    return "\n\n".join(markdowns)


# ---- NEW: schema-agnostic markdown extraction for non-OCRResponse JSONs ----
def _walk_collect_markdownish(obj) -> list[str]:
    """Recursively collect page-level markdown/text-like fields from arbitrary JSON."""
    collected = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = k.lower()
            # Common field names across OCR/LLM outputs
            if lk in {"markdown", "md", "page_markdown", "combined_markdown", "text", "page_text"} and isinstance(v, str):
                collected.append(v)
            else:
                collected.extend(_walk_collect_markdownish(v))
    elif isinstance(obj, list):
        for item in obj:
            collected.extend(_walk_collect_markdownish(item))
    return collected


def get_markdown_from_any_json(json_data) -> str | None:
    """
    Try to coerce arbitrary OCR JSON into a combined markdown string.
    Priority:
      1) If it already has OCRResponse shape, caller should handle that.
      2) Otherwise, recursively scrape markdown/text-like fields and join.
    """
    parts = _walk_collect_markdownish(json_data)
    if parts:
        parts = [p.strip() for p in parts if p and p.strip()]
        return "\n\n".join(parts) if parts else None
    return None
# ---------------------------------------------------------------------------


def extract_from_md(md: str, name: str) -> tuple[str, str, str]:
    pdf_name = name

    # Tighten the prompt: force exact top-level keys and Year field
    prompt = f"""
 You are analyzing a U.S. Federal Reserve FR Y-6 regulatory filing.

    From the text below, extract two structured tables and return them as a JSON object with two keys (IF A NONE VALUE IS FOUND—eg. None, N/A, etc—, REPLACE WITH THE VALUE WITH A null value); also include the bank name and fiscal year in the output:

    1. shareholders — list of:
       - Name and Address
       - Country of Citizenship
       - Number and Percentage of Voting Stock

    2. insiders — list of:
       - Name and Address
       - Principal occupation if other than with Bank Holding Company
       - Title and Position with Bank Holding Company
       - Title and Position with Subsidiaries
       - Title and Position with Other Businesses
       - Percentage of Voting Shares in Bank Holding Company
       - Percentage of Voting Shares in Subsidiaries
       - List names of other companies if 25% or more of voting securities are held

    3. bank_data - list of:
         - Bank Name
         - Year

    Return valid JSON only (no markdown or formatting).

    FR Y-6 OCR TEXT:
---
{md}
"""

    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    output_text = response.text.strip()

    match = re.search(r'```json\s*({.*?})\s*```', output_text, re.DOTALL) or \
            re.search(r'({.*})', output_text, re.DOTALL)
    if not match:
        raise ValueError("Gemini did not return valid JSON")

    tables = json.loads(match.group(1))

    # Extract bank name and year from Gemini output (now enforced by prompt)
    bank_data_list = tables.get("bank_data", [])
    if bank_data_list and isinstance(bank_data_list, list) and bank_data_list:
        bank_data = bank_data_list[0]
        bank_name = bank_data.get("Bank Name") or extract_bank_name(md, pdf_name) or "Unknown"
        year = bank_data.get("Year") or extract_fiscal_year(md, pdf_name) or "Unknown"
    else:
        bank_name = extract_bank_name(md, pdf_name) or "Unknown"
        year = extract_fiscal_year(md, pdf_name) or "Unknown"

    insiders_df = pd.DataFrame(tables.get("insiders", []))
    shareholders_df = pd.DataFrame(tables.get("shareholders", []))

    print("Shareholders", shareholders_df)
    print("Insiders", insiders_df)

    # Table presence computed from actual dataframes
    table_presence = (
        "both" if not insiders_df.empty and not shareholders_df.empty else
        "insiders" if not insiders_df.empty else
        "securities" if not shareholders_df.empty else
        "none"
    )

    base_data = {
        "Bank Name": bank_name,
        "table presence": table_presence,
        "Bank_PDF-Name": pdf_name,
        "Year": year
    }

    if insiders_df.empty:
        insiders_df = pd.DataFrame([base_data])
    else:
        for k, v in base_data.items():
            insiders_df[k] = v

    if shareholders_df.empty:
        shareholders_df = pd.DataFrame([base_data])
    else:
        for k, v in base_data.items():
            shareholders_df[k] = v

    insiders_path = insiders_dir / f"{name}.csv"
    shareholders_path = securities_dir / f"{name}.csv"

    insiders_df.to_csv(insiders_path, index=False)
    shareholders_df.to_csv(shareholders_path, index=False)

    s3.upload_file(str(insiders_path), bucket_name, f"csv_testing/insiders_third/{name}.csv")
    s3.upload_file(str(shareholders_path), bucket_name, f"csv_testing/securities_third/{name}.csv")

    # Delete local files after upload
    insiders_path.unlink(missing_ok=True)
    shareholders_path.unlink(missing_ok=True)

    print("Found year:", year)
    print("Found bank name:", bank_name)
    print(f"✅ Saved: insiders/{name}.csv, securities/{name}.csv")

    return bank_name, year, table_presence


# === MAIN DRIVER ===
def main():
    tracked = load_tracked_files()
    objects = list_all_s3_objects(bucket_name, prefix)

    if not objects:
        print("No objects found in S3 bucket.")
        return

    # Filter to JSON objects and take a deterministic first N (sorted by key)
    json_objects = sorted(
        [o for o in objects if o.get("Key", "").endswith(".json")],
        key=lambda x: x["Key"]
    )[:MAX_FILES]

    print(f"Found {len(objects)} objects under prefix '{prefix}'.")
    print(f"Processing up to {MAX_FILES} JSON files (actually processing {len(json_objects)}).")

    for obj in json_objects:
        key = obj["Key"]

        # Safety: only process the 2023 folder
        if not key.startswith("MistralCapIQUpdated/2023/"):
            continue

        name = key.split("/")[-1].replace(".json", "")
        if tracked.get(name) in {"passed", "failed"}:
            print(f"⏭️ Skipping already processed: {name}")
            continue

        print(f"\n--- Processing: {key} ---")
        try:
            file_obj = s3.get_object(Bucket=bucket_name, Key=key)
            file_content = file_obj["Body"].read().decode("utf-8")

            # Defensive parse
            try:
                json_data = json.loads(file_content)
            except Exception as je:
                print(f"JSON parse error for {name}: {je}")
                update_tracking(name, "failed", f"json parse error: {je}")
                continue

            markdown = None

            # Try OCRResponse if the shape matches
            try:
                if isinstance(json_data, dict) and all(k in json_data for k in ("pages", "model", "usage_info")):
                    ocr_response = OCRResponse.model_validate(json_data)
                    markdown = get_combined_markdown(ocr_response)
                else:
                    # Schema-agnostic fallback
                    markdown = get_markdown_from_any_json(json_data)
            except Exception:
                # If validation or building markdown fails, fallback
                markdown = get_markdown_from_any_json(json_data)

            if not markdown or not markdown.strip():
                raise ValueError("No markdown/text content found in JSON")

            bank_name, year, presence = extract_from_md(markdown, name)
            update_tracking(name, "passed", bank_name=bank_name, year=year, presence=presence)

        except Exception as e:
            print(f"❌ Failed: {name}: {e}")
            update_tracking(name, "failed", str(e))


if __name__ == "__main__":
    main()
