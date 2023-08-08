import requests
import json
import boto3
from botocore import exceptions
import os
from typing import Dict

JSONType = Dict[str, str]

def get_apod_data() -> JSONType:
    """Grabs data from APOD API, and returns in JSON format"""
    api_url = "https://api.nasa.gov/planetary/apod"
    params = {
        "api_key": os.environ.get("api_key"),  # Replace with your API key if required
        "hd": True,  # Request the high-definition image
    }

    response = requests.get(api_url, params=params)
    meta = response.json()

    return meta

def store(s3_bucket: str):

    meta = get_apod_data()
    s3 = boto3.client("s3")


    if meta:

        image_url = meta.get("url")
        if image_url is not None:
            image_response = requests.get(image_url)

            if image_response.status_code == 200:
                image = image_response.content
                file_name = os.path.basename(image_url)

                s3 = boto3.client('s3')
                # Get the file name without the extension
                folder_name = "Images/" + os.path.splitext(file_name)[0]
                # Specify the folder path as part of the object key
                image_key = f"{folder_name}/{file_name}"
                meta_key = f"{folder_name}/data.json"

                # Check if the object already exists in S3
                try:
                    s3.head_object(Bucket=s3_bucket, Key=image_key)
                    print(f"Object '{image_key}' already exists in S3. Action canceled.")
                except exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        try:
                            s3.put_object(
                                Bucket=s3_bucket,
                                Key=image_key,
                                Body=image,
                                ContentType='image/jpeg'  # Update content type if needed
                            )
                            s3.put_object(
                                Bucket=s3_bucket,
                                Key=meta_key,
                                Body=(bytes(json.dumps(meta).encode('UTF-8'))),
                                ContentType="application/json"
                            )

                            print(f"Image and metadata stored in S3: s3://{s3_bucket}/{folder_name}")
                        except Exception as e:
                            print("Error uploading to S3:", e)
                    else:
                        print("Error checking object existence:", e)
            else:
                print("Failed to retrieve from API.")
        else:
            print("Dictionary not found.")

def lambda_handler(event, context):
    store("ada-bucket")