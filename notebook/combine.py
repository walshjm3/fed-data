import os
import boto3
import pandas as pd
from io import StringIO

# Configuration
bucket_name = "fed-data-storage"
prefix = "csv_testing/securities_fourth/"
tables_prefix = "csv_testing/tables/"  # S3 "folder" for final combined CSV
output_csv_name = "all_securities_combined_fourth.csv"  # Local output filename
output_s3_key = f"{tables_prefix}{output_csv_name}"

# Initialize S3 client (make sure your AWS CLI / env creds are configured)
s3 = boto3.client('s3')

def list_csv_files(bucket, prefix):
    """List all CSV files in a given S3 bucket and prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    csv_keys = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(".csv"):
                csv_keys.append(key)
    return csv_keys

def download_csv_from_s3(bucket, key):
    """Download a CSV file from S3 and return a pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(data))

def upload_file_to_s3(local_path, bucket, key):
    """Upload a local file to S3."""
    s3.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": "text/csv"}
    )
    print(f"Uploaded to s3://{bucket}/{key}")

def main():
    all_csv_keys = list_csv_files(bucket_name, prefix)
    print(f"Found {len(all_csv_keys)} CSV files under '{prefix}'.")

    all_dfs = []
    for key in all_csv_keys:
        try:
            df = download_csv_from_s3(bucket_name, key)
            all_dfs.append(df)
            print(f"Loaded: {key} ({len(df)} rows)")
        except Exception as e:
            print(f"Failed to load {key}: {e}")

    if not all_dfs:
        print("No CSVs loaded. Exiting.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined rows: {len(combined_df)}")

    # Save locally
    combined_df.to_csv(output_csv_name, index=False)
    print(f"Saved combined CSV to: {output_csv_name}")

    # Upload to S3 at csv_testing/tables/
    upload_file_to_s3(output_csv_name, bucket_name, output_s3_key)

if __name__ == "__main__":
    main()
