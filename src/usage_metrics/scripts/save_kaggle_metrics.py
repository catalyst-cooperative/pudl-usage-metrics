"""This script pull Kaggle traffic metrics and saves them to a GC Bucket."""

import json
import logging
import sys
from datetime import date

from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi

logger = logging.getLogger()
logging.basicConfig(level="INFO")


def get_kaggle_logs() -> str:
    """Get PUDL project usage metadata from Kaggle site."""
    api = KaggleApi()
    kaggle_owner = "catalystcooperative"
    kaggle_dataset = "pudl-project"

    metadata = api.metadata_get(kaggle_owner, kaggle_dataset)
    metadata.update({"metrics_date": date.today().strftime("%Y-%m-%d")})
    return json.dumps(metadata)


def upload_to_bucket(data):
    """Upload a gcp object."""
    bucket_name = "pudl-usage-metrics-archives.catalyst.coop"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = f"kaggle/{date.today().strftime('%Y-%m-%d')}.json"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

    logger.info(f"Uploaded today's data to {blob_name}.")


def save_metrics():
    """Save github traffic metrics to google cloud bucket."""
    kaggle_metrics = get_kaggle_logs()
    upload_to_bucket(kaggle_metrics)


if __name__ == "__main__":
    sys.exit(save_metrics())
