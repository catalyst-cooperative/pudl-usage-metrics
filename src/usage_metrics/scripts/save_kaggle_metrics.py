"""This script pull Kaggle traffic metrics and saves them to a GC Bucket."""

import json
import logging
import sys
from datetime import date

from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi

KAGGLE_OWNER = "catalystcooperative"
KAGGLE_DATASET = "pudl-project"
OWNER = "catalyst-cooperative"
REPO = "pudl"
BUCKET_NAME = "kaggle-metrics"

logger = logging.getLogger()
logging.basicConfig(level="INFO")


def get_kaggle_logs() -> str:
    """Get PUDL project usage metadata from Kaggle site."""
    api = KaggleApi()

    metadata = api.metadata_get(KAGGLE_OWNER, KAGGLE_DATASET)
    metadata.update({"metrics_date": date.today().strftime("%Y-%m-%d")})
    return json.dumps(metadata)


def upload_to_bucket(data):
    """Upload a gcp object."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"kaggle-metrics-{date.today().strftime('%Y-%m-%d')}.json"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

    logger.info(f"Uploaded today's data to {blob_name}.")


def save_metrics():
    """Save github traffic metrics to google cloud bucket."""
    kaggle_metrics = get_kaggle_logs()
    upload_to_bucket(kaggle_metrics)


if __name__ == "__main__":
    sys.exit(save_metrics())
