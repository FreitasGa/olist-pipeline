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

    KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
    KAGGLE_KEY = os.getenv("KAGGLE_KEY")
    DATASET_URL = os.getenv("DATASET_URL")
    DATASET_DIR = os.getenv("DATASET_DIR")
    BUCKET_NAME = os.getenv("BUCKET_NAME")

    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        logger.error("Kaggle credentials not found")
        raise

    if not DATASET_URL or not DATASET_DIR:
        logger.error("Dataset information not found")
        raise

    if not BUCKET_NAME:
        logger.error("S3 bucket name not found")
        raise

    os.makedirs(DATASET_DIR, exist_ok=True)

    try:
        logger.info("Downloading dataset")

        response = requests.get(
            DATASET_URL,
            auth=(KAGGLE_USERNAME, KAGGLE_KEY),
            stream=True
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

        client = boto3.client("s3")

        logger.info("Uploading files to S3")

        for path in paths:
            client.upload_file(
                path,
                BUCKET_NAME,
                os.path.basename(path)
            )

        logger.info(f"Ingestion complete, at {datetime.datetime.now()}")
    except Exception as e:
        logger.error(f"Ingestion failed, at {datetime.datetime.now()}")
        logger.error(e)
        raise
