import requests
import json
import boto3
from botocore import exceptions
import os
import logging
import time
from typing import Dict

# Set up logging for CloudWatch
logger = logging.getLogger() 
logger.setLevel(logging.INFO)   

JSONType = Dict[str, str] # Define a JSON for type-checking
api_key = os.environ.get("api_key")

def get_apod_data() -> JSONType:
    """Grabs data from APOD API, and returns in JSON format"""
    api_url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"

    try:
        response = requests.get(api_url, params={"hd": True})
        response.raise_for_status()  # Raises an exception if response status is not 2xx
    except requests.RequestException as e:
        logger.error(f"Error fetching from API: {e}")
        exit(1)

    data = response.json()  # Assign data variable to the JSON Object

    return data

def store_apod(s3_bucket: str, max_retries=3):
    """Stores JSON and Image into an S3 Bucket"""
    data = get_apod_data()
    s3 = boto3.client("s3")

    if data:

        image_url = data.get("hdurl")  # Grabs the HD image URL

        if image_url:
            
            try:
                response = requests.get(image_url)
                response.raise_for_status()  # Raises an exception if response status is not 2xx
                image = response.content
            except requests.RequestException as e:
                logger.error(f"Error fetching image from URL: {e}")
                exit(1)
            else:
                s3 = boto3.client('s3')  # Initialize S3 Client
                file_name = os.path.basename(image_url)
                folder_name = "Images/APOD/" + os.path.splitext(file_name)[0]  # Set the folder name which the files will be nested in
                image_key = f"{folder_name}/{file_name}"
                data_key = f"{folder_name}/data.json"

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
                                Key=data_key,
                                Body=(bytes(json.dumps(data).encode('UTF-8'))),  # Dumps the data variable into a JSON file which is then uploaded
                                ContentType="application/json"  # Uploads as a JSON file
                            )

                            logger.info(f"Image and datadata stored in S3: s3://{s3_bucket}/{folder_name}")
                        except Exception as e:
                            logger.error("Error uploading to S3:", e)
                    else:
                        logger.error("Error checking object existence:", e)
        else:
            logger.error("Failed to retrieve image content.")
    else:
        logger.info("API data variable is empty.")
