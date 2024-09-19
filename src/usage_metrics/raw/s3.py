"""Extract data from S3 logs."""

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


class S3Extractor(GCSExtractor):
    """Extractor for S3 logs."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.dataset_name = "pudl_s3_logs"
        self.bucket_name = "pudl-s3-logs.catalyst.coop"
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
        return [blob for blob in blobs if blob.name.startswith(partition_dates)]

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        return pd.read_csv(file_path, delimiter=" ", header=None)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "s3"},
)
def raw_s3_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract S3 logs from sub-daily files and return one daily DataFrame."""
    return S3Extractor().extract(context)
