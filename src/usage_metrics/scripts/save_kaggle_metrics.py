"""This script pull Kaggle traffic metrics and saves them to a GC Bucket."""

import json
import logging
import sys
import tempfile
from datetime import date
from pathlib import Path

from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi

logger = logging.getLogger()
logging.basicConfig(level="INFO")


def get_kaggle_dataset_metadata() -> str:
    """Get PUDL project usage metadata from Kaggle site."""
    # Instantiate and authenticate to the Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Create a temporary directory to save the file to.

    with tempfile.TemporaryDirectory() as tmpdir:
        api.dataset_metadata(
            "catalystcooperative/pudl-project", tmpdir
        )  # Download metrics to CWD.

        json_path = Path(tmpdir, "dataset-metadata.json")
        with Path.open(json_path) as file:
            metadata_str = json.load(
                file
            )  # Kaggle downloads to the current working directory.

    metadata = json.loads(metadata_str)  # Fix bad formatting in original JSON file
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
    kaggle_dataset_metrics = get_kaggle_dataset_metadata()
    upload_to_bucket(kaggle_dataset_metrics)


if __name__ == "__main__":
    sys.exit(save_metrics())
