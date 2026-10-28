"""Extract data from Zenodo logs from archived JSON files in GCS.

Each JSON file has metadata on all records in a version. These files are saved by
scripts/save_zenodo_metrics.py.
"""

import json
import re
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    asset,
)
from google.cloud import storage
from pydantic import BaseModel

from usage_metrics.paths import PUDL_METRICS_ARCHIVES_BUCKET
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
    publication_date: datetime | None = None


class ZenodoExtractor(GCSExtractor):
    """Extractor for Zenodo logs."""

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = "pudl_zenodo_logs"
        self.bucket_name = PUDL_METRICS_ARCHIVES_BUCKET
        super().__init__(*args, **kwargs)

    def filter_blobs(
        self, context: AssetExecutionContext, blobs: Iterable[storage.Blob]
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
        file_name_prefixes = tuple(f"zenodo/{date}-" for date in partition_dates)
        pattern = re.compile(r"\d{4}-\d{2}-\d{2}-\d+\.json$")

        filtered_blobs = [
            blob
            for blob in blobs
            if blob.name is not None
            and pattern.search(blob.name)
            and blob.name.startswith(file_name_prefixes)
        ]
        return filtered_blobs

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        with Path.open(file_path) as data_file:
            data_json = json.load(data_file)

        df = pd.json_normalize(data_json["hits"]["hits"])
        # Add in date of metrics column from file name
        date_match = re.search(r"\d{4}-\d{2}-\d{2}", str(file_path))
        if date_match is None:
            raise ValueError(f"Could not find a date in file path {file_path}")
        df["metrics_date"] = date_match.group()
        # A file is named for the id of the latest version of the record it archives,
        # which grows as versions are published, so it orders the archives of one day.
        id_match = re.search(r"\d{4}-\d{2}-\d{2}-(\d+)\.json$", str(file_path))
        if id_match is None:
            raise ValueError(f"Could not find a record id in file path {file_path}")
        df["source_record_id"] = int(id_match.group(1))
        return df


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "zenodo"},
)
def raw_zenodo_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract Zenodo logs from sub-daily files and return one weekly DataFrame."""
    return ZenodoExtractor().extract(context)
