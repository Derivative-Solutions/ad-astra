import boto3
from botocore.exceptions import NoCredentialsError
from typing import Optional, Dict
import json

def get_apod(s3_bucket: str) -> Optional[Dict]:
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix="Images/APOD/")
        objects = response.get('Contents', [])
        
        if not objects:
            print("No objects within the folder.")
            return None
        
        # Sort objects by last modified timestamp in descending order
        objects.sort(key=lambda obj: obj['LastModified'], reverse=True)
        
        key = objects[0]['Key']
        
        # Fetch the latest APOD data from the S3 object
        object = s3.get_object(Bucket=s3_bucket, Key=key)
        # Assuming obj is the dictionary you provided
        object = object['Body'].read().decode('utf-8')
        data = json.loads(object)

        if 'hdurl' in data:
            hdurl = data['hdurl']
        else:
            hdurl = None

        if 'url' in data:
            url = data['url']
        else:
            url = None

        if hdurl is not None:
            image_url = hdurl
        elif url is not None:
            image_url = url
        else:
            print("No URL found in S3 JSON file.")


        apod_data = {
            'title': data['title'],
            'copyright': data['copyright'],
            'date': data['date'],
            'explanation': data['explanation'],
            "url": image_url
        }


        return apod_data
    
    except NoCredentialsError as e:
        print(f"Credentials not found, please check Django environment variables: {e}")
        return None