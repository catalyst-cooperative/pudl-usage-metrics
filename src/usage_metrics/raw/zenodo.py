"""Extract data from Zenodo logs."""

import re
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)
from google.api_core.page_iterator import HTTPIterator
from google.cloud import storage

from usage_metrics.raw.extract import GCSExtractor


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
        file_name_prefixes = tuple(f"zenodo/{date}.csv" for date in partition_dates)

        blobs = [blob for blob in blobs if blob.name in file_name_prefixes]
        return blobs

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        df = pd.read_csv(file_path)
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
