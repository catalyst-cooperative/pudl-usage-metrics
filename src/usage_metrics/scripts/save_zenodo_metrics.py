"""This script pull Zenodo traffic metrics and saves them to a GCS Bucket."""

import json
import logging
import sys
from datetime import date, datetime
from typing import Annotated

import pandas as pd
import requests
from google.cloud import storage
from pydantic import BaseModel, StringConstraints

from usage_metrics.helpers import retry_request

Doi = Annotated[str, StringConstraints(pattern=r"10\.5281/zenodo\.\d+")]
SandboxDoi = Annotated[str, StringConstraints(pattern=r"10\.5072/zenodo\.\d+")]

logger = logging.getLogger()
logging.basicConfig(level="INFO")


class CommunityMetadata(BaseModel):
    """Pydantic model representing Zenodo deposition metadata from the communities endpoint.

    See https://developers.zenodo.org/#representation.
    """

    created: datetime = None
    modified: datetime = None
    recid: str
    conceptrecid: str
    doi: Doi | SandboxDoi | None = None
    conceptdoi: Doi | SandboxDoi | None = None
    doi_url: str
    title: str
    updated: datetime = None
    revision: int

    @classmethod
    def check_empty_string(cls, doi: str):  # noqa: N805
        """Sometimes zenodo returns an empty string for the `doi`. Convert to None."""
        if doi == "":
            return


def save_zenodo_logs() -> pd.DataFrame():
    """Get JSONs of Zenodo metrics for all Catalyst records and upload to GCS.

    Get metrics for all versions in the Catalyst Cooperative Zenodo community locally,
    and then upload to the sources.catalyst.coop GCS bucket.
    """

    def get_all_records(url: str, page: int = 1, page_size: int = 25):
        """Iterate through pages and get all records.

        Zenodo requires authentication for API requests over a page size of 25.
        Instead of navigating authentication, this function grabs all the records
        by iterating through several pages at the page size limit.
        Also return the JSON itself, which is used to maintain the original format
        of data in cases where we are querying more than one page.
        """
        all_dataset_records = []

        full_url = f"{url}?page={page}&size={page_size}&sort=newest"

        while True:
            logger.debug(f"Archiving {full_url}")
            record_json = fetch_json(full_url)
            dataset_records = record_json["hits"]["hits"]
            all_dataset_records += dataset_records

            links = record_json.get("links")
            if links and links.get("next"):
                full_url = links["next"]
                continue
            break  # Exit loop when all records extracted

        record_json["hits"]["hits"] = (
            all_dataset_records  # Add all versions into one JSON to preserve format
        )
        return record_json

    @retry_request(retries=3, delay=1, backoff=2)
    def fetch_json(url):
        """Get and load a JSON file from Zenodo."""
        record_versions = requests.get(url=url, timeout=100)
        return record_versions.json()

    bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # First, get metadata on all the datasets in the Catalyst Cooperative community
    # If a dataset is not added to Cat Community, it will not show up here.
    all_datasets_json = get_all_records(
        url="https://zenodo.org/api/communities/14454015-63f1-4f05-80fd-1a9b07593c9e/records"
    )
    all_dataset_records = [
        CommunityMetadata(**record) for record in all_datasets_json["hits"]["hits"]
    ]

    for record in all_dataset_records:
        logger.info(f"Getting usage metrics for {record.title}")
        # For each dataset in the community, get all archived versions and their
        # corresponding metrics.
        versions_url = f"https://zenodo.org/api/records/{record.recid}/versions"
        record_versions_json = get_all_records(url=versions_url)
        versions_metadata = json.dumps(record_versions_json)
        blob_name = f"zenodo/{date.today().strftime('%Y-%m-%d')}-{record.recid}.json"
        upload_to_bucket(bucket=bucket, blob_name=blob_name, data=versions_metadata)


def upload_to_bucket(
    bucket: storage.Client.bucket, blob_name: str, data: pd.DataFrame
) -> None:
    """Upload a GCP object to a selected bucket."""
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

    logger.info(f"Uploaded {blob_name} to GCS bucket.")


if __name__ == "__main__":
    sys.exit(save_zenodo_logs())
