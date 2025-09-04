"""Extract data from Kaggle logs."""

import json
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


class KaggleExtractor(GCSExtractor):
    """Extractor for Kaggle logs."""

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = "pudl_kaggle_logs"
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
        file_name_prefixes = tuple(f"kaggle/{date}.json" for date in partition_dates)

        blobs = [blob for blob in blobs if blob.name in file_name_prefixes]
        return blobs

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in JSON file as dataframe."""
        with Path.open(file_path, "r") as file:
            data = json.load(file)
            if data["hasErrorMessage"]:
                # Catch error message contained in Kaggle API JSON response
                raise AssertionError(f"Data contains error {data['errorMessage']}")
            df = pd.json_normalize(data["info"])
            df["metrics_date"] = data["metrics_date"]
            return df


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "kaggle"},
)
def raw_kaggle_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract Kaggle logs from daily files and return one weekly DataFrame."""
    return KaggleExtractor().extract(context)
