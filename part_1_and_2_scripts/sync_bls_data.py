import requests
import boto3
from bs4 import BeautifulSoup
import hashlib
import os
from urllib.parse import urljoin

# --- Configuration ---
BLS_DATA_URL = "https://download.bls.gov/pub/time.series/pr/"
BUCKET_NAME = "mayank-rearc-quest-2025"
S3_FOLDER = "pr/"


def get_s3_object_etags(s3_client, bucket, prefix):
    etags = {}
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                etags[obj['Key']] = obj['ETag'].strip('"')
    return etags


def calculate_file_md5(file_content):
    return hashlib.md5(file_content).hexdigest()


def sync_bls_data_to_s3():
    print("Starting data sync process...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }

    s3 = boto3.client('s3')

    existing_etags = get_s3_object_etags(s3, BUCKET_NAME, S3_FOLDER)
    print(f"Found {len(existing_etags)} existing objects in S3 under '{S3_FOLDER}'.")

    response = requests.get(BLS_DATA_URL, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')

    for link in soup.find_all('a'):
        href = link.get('href')
        if href and not href.startswith('?C') and href != '../' and not href.endswith('/'):
            file_url = urljoin(BLS_DATA_URL, href)
            
            # Extract just the filename for the S3 key
            filename = href.split('/')[-1]
            s3_key = os.path.join(S3_FOLDER, filename)

            print(f"Processing file: {filename}...")
            
            file_response = requests.get(file_url, headers=headers)
            if file_response.status_code == 200:
                file_content = file_response.content
                local_md5 = calculate_file_md5(file_content)

                if s3_key not in existing_etags or existing_etags[s3_key] != local_md5:
                    print(f"Uploading {filename} to s3://{BUCKET_NAME}/{s3_key}")
                    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=file_content)
                else:
                    print(f"File {filename} is already up-to-date in S3. Skipping.")
            else:
                print(f"Failed to download {file_url}. Status code: {file_response.status_code}")

    print("\nData sync process completed.")


if __name__ == "__main__":
    sync_bls_data_to_s3()