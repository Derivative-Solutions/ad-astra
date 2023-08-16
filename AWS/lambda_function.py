import logging

from store_apod import store_apod
from store_mars import store_mars

# Set up logging for CloudWatch
logger = logging.getLogger() 
logger.setLevel(logging.INFO)
bucket = "ad-astra-bucket"

def lambda_handler(event, context):
    """Run the function via the lambda handler"""
    try:
        store_apod(bucket)
        store_mars(bucket)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    