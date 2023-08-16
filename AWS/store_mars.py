import requests
import json
import os
import logging
import boto3
from botocore import exceptions
from typing import Dict, List

# Set up logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)
api_key = os.environ.get("api_key")

JSONType = Dict[str, str]  # Define a JSON for type-checking

def get_mars_photo_data() -> List[Dict[str, str]]:
    """Grabs data from the Mars Photo API, and returns in JSON format"""
    api_url = f"https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/latest_photos?api_key={api_key}"

    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raises an exception if response status is not 2xx
    except requests.RequestException as e:
        logger.error(f"Error fetching from API: {e}")
        raise

    raw_data = response.json()  # Assign data variable to the JSON Object
    data = []

    for i in range(3):
        latest_photo = raw_data.get("latest_photos")[i]
        sol = latest_photo["sol"]
        earth_date = latest_photo["earth_date"]
        camera_name = latest_photo["camera"]["full_name"]
        image_url = latest_photo["img_src"]

        photo_data = {
            "photo_id": i,
            "sol": sol,
            "earth_date": earth_date,
            "camera_name": camera_name,
            "image_url": image_url
        }
        data.append(photo_data)

    logger.info(f"Fetched {len(data)} Mars photos.")
    return data

def store_mars(s3_bucket: str, max_retries=3):
    """Stores JSON and Image into an S3 Bucket"""
    data = get_mars_photo_data()
    s3 = boto3.client("s3")

    if data:
        for photo_info in data:
            sol = photo_info["sol"]
            image_url = photo_info["image_url"]
            photo_id = photo_info["photo_id"]
            try:
                response = requests.get(image_url)
                response.raise_for_status()  # Raises an exception if response status is not 2xx
                image = response.content
            except requests.RequestException as e:
                logger.error(f"Error fetching image from URL: {e}")
                exit(1)


            file_name = os.path.basename(image_url)
            folder_name = f"Images/Mars_Rovers/{sol}/{photo_id}"  # Set the folder name which the files will be nested in
            image_key = f"{folder_name}/{file_name}"
            data_key = f"{folder_name}/data.json"


            try:
                s3.head_object(Bucket=s3_bucket, Key=image_key)  # Check if the object already exists in S3
                logger.info(f"Object '{image_key}' already exists in S3. Action canceled.")
                break
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
                            Body=(bytes(json.dumps(photo_info).encode('UTF-8'))),
                            ContentType="application/json"
                        )
                        logger.info(f"Image and data.json stored in S3: s3://{s3_bucket}/{folder_name}")
                    except Exception as e:
                        logger.error("Error uploading to S3:", e)

                else:
                    logger.error("Error checking object existence:", e)
    else:
        logger.error("API data variable is empty.")