"""This script pull Zenodo traffic metrics and saves them to a GCS Bucket."""

import json
import logging
import sys
from datetime import date, datetime
from typing import Annotated

import pandas as pd
from google.cloud import storage
from pydantic import BaseModel, StringConstraints

from usage_metrics.helpers import make_request

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


def get_zenodo_logs() -> pd.DataFrame():
    """Download JSONs of metrics for all versions in the Catalyst Cooperative Zenodo community."""
    bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    community_url = "https://zenodo.org/api/communities/14454015-63f1-4f05-80fd-1a9b07593c9e/records"
    # First, get metadata on all the datasets in the Catalyst Cooperative community
    community_records = make_request(url=community_url)
    dataset_records = community_records.json()["hits"]["hits"]
    dataset_records = [CommunityMetadata(**record) for record in dataset_records]

    for record in dataset_records:
        logger.info(f"Getting usage metrics for {record.title}")
        # For each dataset in the community, get all archived versions and their
        # corresponding metrics.
        versions_url = f"https://zenodo.org/api/records/{record.recid}/versions"
        record_versions = make_request(url=versions_url).json()
        versions_metadata = json.dumps(record_versions)
        blob_name = f"zenodo/{date.today().strftime('%Y-%m-%d')}-{record.recid}.json"
        upload_to_bucket(bucket=bucket, blob_name=blob_name, data=versions_metadata)


def upload_to_bucket(
    bucket: storage.Client.bucket, blob_name: str, data: pd.DataFrame
) -> None:
    """Upload a gcp object."""
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

    logger.info(f"Uploaded {blob_name} to GCS bucket.")


def save_metrics():
    """Save Zenodo traffic metrics to google cloud bucket."""
    get_zenodo_logs()


if __name__ == "__main__":
    sys.exit(save_metrics())
