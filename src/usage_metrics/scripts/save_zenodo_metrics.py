"""This script pull github traffic metrics and saves them to a GC Bucket."""

import logging
import sys
from datetime import date, datetime
from typing import Annotated

import pandas as pd
import requests
from google.cloud import storage
from pydantic import BaseModel, StringConstraints

Doi = Annotated[str, StringConstraints(pattern=r"10\.5281/zenodo\.\d+")]
SandboxDoi = Annotated[str, StringConstraints(pattern=r"10\.5072/zenodo\.\d+")]

logger = logging.getLogger()
logging.basicConfig(level="INFO")


class ZenodoStats(BaseModel):
    """Pydantic model representing Zenodo usage stats.

    See https://developers.zenodo.org/#representation.
    """

    downloads: int
    unique_downloads: int
    views: int
    unique_views: int
    version_downloads: int
    version_unique_downloads: int
    version_unique_views: int
    version_views: int


class ZenodoMetadata(BaseModel):
    """Pydantic model representing relevant Zenodo metadata.

    See https://developers.zenodo.org/#representation.
    """

    version: str | None = None
    publication_date: datetime = None


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
    stats: ZenodoStats
    metadata: ZenodoMetadata

    @classmethod
    def check_empty_string(cls, doi: str):  # noqa: N805
        """Sometimes zenodo returns an empty string for the `doi`. Convert to None."""
        if doi == "":
            return


def get_zenodo_logs() -> pd.DataFrame():
    """Get all metrics for all versions of records in the Catalyst Cooperative Zenodo community."""
    community_url = "https://zenodo.org/api/communities/14454015-63f1-4f05-80fd-1a9b07593c9e/records"
    # First, get metadata on all the datasets in the Catalyst Cooperative community
    community_records = requests.get(community_url, timeout=100)
    dataset_records = community_records.json()["hits"]["hits"]
    dataset_records = [CommunityMetadata(**record) for record in dataset_records]
    stats_dfs = []

    for record in dataset_records:
        logger.info(f"Getting usage metrics for {record.title}")
        # For each dataset in the community, get all archived versions and their
        # corresponding metrics.
        versions_url = f"https://zenodo.org/api/records/{record.recid}/versions"
        record_versions = requests.get(versions_url, timeout=100)
        version_records = record_versions.json()["hits"]["hits"]
        version_records = [
            CommunityMetadata(**version_record) for version_record in version_records
        ]
        version_df = pd.DataFrame(
            [
                dict(
                    version_record.stats.dict(),
                    doi=version_record.doi,
                    title=version_record.title,
                )
                | version_record.metadata.dict()
                for version_record in version_records
            ]
        )
        if not version_df.empty:
            stats_dfs.append(version_df)
    return pd.concat(stats_dfs)


def upload_to_bucket(data: pd.DataFrame) -> None:
    """Upload a gcp object."""
    bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(f"zenodo/{date.today().strftime('%Y-%m-%d')}.csv")
    blob.upload_from_string(data.to_csv(index=False), "text/csv")

    logger.info("Uploaded data to GCS bucket.")


def save_metrics():
    """Save Zenodo traffic metrics to google cloud bucket."""
    metrics_df = get_zenodo_logs()
    upload_to_bucket(metrics_df)


if __name__ == "__main__":
    sys.exit(save_metrics())
