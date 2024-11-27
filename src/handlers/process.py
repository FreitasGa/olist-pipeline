import datetime
import logging
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    logger.info(f"Processing dataset, at {datetime.datetime.now()}")

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION")

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
        logger.error("AWS credentials not found")
        raise
