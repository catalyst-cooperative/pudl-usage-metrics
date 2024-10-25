"""Extract data from Zenodo logs from archived JSON files in GCS.

Each JSON file has metadata on all records in a version. These files are saved by
scripts/save_zenodo_metrics.py.
"""

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)
from google.api_core.page_iterator import HTTPIterator
from google.cloud import storage
from pydantic import BaseModel

from usage_metrics.raw.extract import GCSExtractor


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


class ZenodoExtractor(GCSExtractor):
    """Extractor for Zenodo logs."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.dataset_name = "pudl_zenodo_logs"
        self.bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
        super().__init__(*args, **kwargs)

    def filter_blobs(
        self, context: AssetExecutionContext, blobs: HTTPIterator
    ) -> list[storage.Blob]:
        """From all possible files in a bucket, filter to include relevant ones.

        Args:
            context: The Dagster asset execution context
            blobs: the list of all file blobs in the bucket, returned by bucket.list_blobs()

        Returns:
            A list of blobs to be downloaded.
        """
        week_start_date_str = context.partition_key
        week_date_range = pd.date_range(start=week_start_date_str, periods=7, freq="D")
        partition_dates = tuple(week_date_range.strftime("%Y-%m-%d"))

        # Construct regex query for zenodo/YYYYMMDD-VERSIONID.json
        # (ignoring older CSV archives)
        # and only search for files in date range
        blobs = [
            blob
            for blob in blobs
            if re.search(r"zenodo\/\d{4}-\d{2}-\d{2}-\d+\.json$", blob.name)
            and any(partition_date in blob.name for partition_date in partition_dates)
        ]
        return blobs

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        with Path.open(file_path) as data_file:
            data_json = json.load(data_file)
        df = pd.json_normalize(data_json["hits"]["hits"])
        # Add in date of metrics column from file name
        df["metrics_date"] = re.search(r"\d{4}-\d{2}-\d{2}", str(file_path)).group()
        return df


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "zenodo"},
)
def raw_zenodo_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract Zenodo logs from sub-daily files and return one weekly DataFrame."""
    return ZenodoExtractor().extract(context)
