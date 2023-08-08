import requests
import json
import boto3
from botocore import exceptions
import os
import logging
from typing import Dict

# Set up logging for CloudWatch
logger = logging.getLogger() 
logger.setLevel(logging.INFO)   

JSONType = Dict[str, str] # Define a JSON for type-checking

def get_apod_data() -> JSONType:
    """Grabs data from APOD API, and returns in JSON format"""
    api_url = "https://api.nasa.gov/planetary/apod"
    params = {
        "api_key": os.environ.get("api_key"),  # This key is encrypted in the Lambda Environment Variables
        "hd": True,  # Request the high-definition image
    }

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raises an exception if response status is not 2xx
    except requests.RequestException as e:
        logger.error(f"Error fetching from API: {e}")
        exit(1)

    meta = response.json()  # Assign metadata variable to the JSON Object

    return meta

def store(s3_bucket: str):
    """Stores JSON and Image into an S3 Bucket"""
    meta = get_apod_data()
    s3 = boto3.client("s3")

    if meta:

        image_url = meta.get("hdurl")  # Grabs the HD image URL
        if image_url is None:
            try:
                image_url = meta.get("url")
                logger.info("Could not find 'hdurl' as a key in 'meta', switching to backup link.")
            except Exception as e:
                logger.error(f"Error fetching URL: {e}")
        else:
            image_response = requests.get(image_url)

            if image_response.status_code == 200:
                image = image_response.content
                file_name = os.path.basename(image_url)

                s3 = boto3.client('s3')  # Initialize S3 Client
                folder_name = "Images/" + os.path.splitext(file_name)[0]  # Set the folder name which the files will be nested in
                image_key = f"{folder_name}/{file_name}"
                meta_key = f"{folder_name}/data.json"

                try:
                    s3.head_object(Bucket=s3_bucket, Key=image_key)  # Check if the object already exists in S3
                    logger.info(f"Object '{image_key}' already exists in S3. Action canceled.")
                except exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        try:
                            s3.put_object(
                                Bucket=s3_bucket,
                                Key=image_key,
                                Body=image,
                                ContentType='image/jpeg'  # Uploads as a JPEG image file
                            )
                            s3.put_object(
                                Bucket=s3_bucket,
                                Key=meta_key,
                                Body=(bytes(json.dumps(meta).encode('UTF-8'))),  # Dumps the meta variable into a JSON file which is then uploaded
                                ContentType="application/json"  # Uploads as a JSON file
                            )

                            logger.info(f"Image and metadata stored in S3: s3://{s3_bucket}/{folder_name}")
                        except Exception as e:
                            logger.error("Error uploading to S3:", e)
                    else:
                        logger.error("Error checking object existence:", e)
            else:
                logger.error("Failed to retrieve image content.")
    else:
        logger.info("API data variable is empty.")

def lambda_handler(event, context):
    try:
        store("ad-astra-bucket")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
