import datetime
import io
import logging
import os
import zipfile
import requests

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    logger.info(f"Ingesting dataset, at {datetime.datetime.now()}")

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION")
    KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
    KAGGLE_KEY = os.getenv("KAGGLE_KEY")
    DATASET_URL = os.getenv("DATASET_URL")
    DATASET_DIR = os.getenv("DATASET_DIR")
    AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
        logger.error("AWS credentials not found")
        return

    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        logger.error("Kaggle credentials not found")
        return

    if not DATASET_URL or not DATASET_DIR:
        logger.error("Dataset information not found")
        return

    if not AWS_S3_BUCKET_NAME:
        logger.error("S3 bucket name not found")
        return

    os.makedirs(DATASET_DIR, exist_ok=True)

    logger.info("Downloading dataset")

    response = requests.get(
        DATASET_URL, auth=(KAGGLE_USERNAME, KAGGLE_KEY), stream=True
    )

    if response.status_code != 200:
        logger.error("Failed to download dataset")
        return

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(DATASET_DIR)

    logger.info("Listing files in dataset")

    paths = []
    for root, dirs, files in os.walk(DATASET_DIR):
        for file in files:
            paths.append(os.path.join(root, file))

    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    logger.info("Uploading files to S3")

    for path in paths:
        client.upload_file(path, AWS_S3_BUCKET_NAME, os.path.basename(path))

    logger.info("Ingestion complete")
