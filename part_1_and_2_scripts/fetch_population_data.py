import requests
import boto3
import json

# --- Configuration ---
API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
BUCKET_NAME = "mayank-rearc-quest-2025"
S3_KEY = "population/us_population_data.json"

def fetch_population_data_and_upload():
    """
    Fetches US population data and uploads it to S3.
    """
    try:
        print("Fetching population data from API...")
        response = requests.get(API_URL)
        # This will raise an error if the API call is unsuccessful
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched data for {len(data['data'])} records.")

        # Initialize the S3 client
        s3 = boto3.client('s3')

        # Upload the JSON data to S3. 
        print(f"Uploading data to s3://{BUCKET_NAME}/{S3_KEY}...")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=S3_KEY,
            Body=json.dumps(data, indent=4)
        )
        print("Upload complete.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    fetch_population_data_and_upload()