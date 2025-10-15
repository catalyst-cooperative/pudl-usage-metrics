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

    def load_file(
        self, context: AssetExecutionContext, file_path: Path
    ) -> pd.DataFrame:
        """Read in JSON file as dataframe."""
        with Path.open(file_path, "r") as file:
            data = json.load(file)
            if data.get("hasErrorMessage"):
                # Catch error message contained in Kaggle API JSON response
                raise AssertionError(f"Data contains error {data['errorMessage']}")
            # The format of the input data changed after a change in the archiving method
            # for the 9-21-2025 partition.
            # Prior to this partition, we had everything nested under the "info" key,
            # except for metrics_date. In the newer data, there isn't the same
            # nesting structure and all fields are housed in the top-level.
            if pd.to_datetime(context.partition_key) < pd.to_datetime("2025-09-21"):
                df = pd.json_normalize(data["info"])
                df["metrics_date"] = data["metrics_date"]
            else:
                df = pd.json_normalize(data)
            return df

    def extract_logs_into_list(
        self, context: AssetExecutionContext, file_paths: list[Path]
    ) -> list[pd.DataFrame]:
        """Read files into a list of Pandas DataFrames, passing context into load method."""
        dict_dfs = []
        for path in file_paths:
            try:
                dict_dfs.append(self.load_file(context, path))
            except pd.errors.EmptyDataError:
                context.log.warnings(f"{path} is an empty file, couldn't read.")
        return dict_dfs


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "kaggle"},
)
def raw_kaggle_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract Kaggle logs from daily files and return one weekly DataFrame."""
    return KaggleExtractor().extract(context)
