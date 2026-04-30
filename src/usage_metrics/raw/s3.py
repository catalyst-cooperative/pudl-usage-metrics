"""Extract data from S3 logs."""

from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    asset,
)
from google.api_core.page_iterator import HTTPIterator
from google.cloud import storage

from usage_metrics.raw.extract import GCSExtractor


class S3Extractor(GCSExtractor):
    """Extractor for S3 logs stored in GCS."""

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = "pudl_s3_logs"
        self.bucket_name = "pudl-s3-logs.catalyst.coop"
        super().__init__(*args, **kwargs)

    def filter_blobs(
        self, context: AssetExecutionContext, blobs: HTTPIterator
    ) -> list[storage.Blob]:
        """From all possible files in a bucket, filter to include relevant ones.

        Note that the timestamp on the S3 file name corresponds to the end of the window
        in which the logs were produced, meaning that logs can sometimes contain data
        from more than one day. We read these records in based on the file name
        and use the time column as the referent timestamp, so this can look unusual in
        the context of examining a single partition but does not cause any issues in
        the overall complete timeseries analysis.

        Args:
            context: The Dagster asset execution context
            blobs: the list of all file blobs in the bucket, returned by bucket.list_blobs()

        Returns:
            A list of blobs to be downloaded.
        """
        day_start_date_str = context.partition_key
        partition_date = datetime.strptime(day_start_date_str, "%Y-%m-%d").strftime(
            "%Y-%m-%d"
        )
        return [blob for blob in blobs if blob.name.startswith(partition_date)]

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        try:
            return pd.read_csv(file_path, delimiter=" ", header=None)
        except pd.errors.ParserError as e:
            # Handle one day of weird edge cases where new column added mid log file hrmph
            # This happens in more than 4 files, so we filter by day rather than identifying the exact files
            # and force these files to have 28 columns rather than the inferred and error-causing 27 found
            # in the first row of these files.
            if "2026-02-25" in file_path.stem:
                return pd.read_csv(
                    file_path, delimiter=" ", header=None, names=range(28)
                )
            e.add_note(
                f"Extraction failed for file: {file_path}"
            )  # Otherwise identify troublesome file
            raise


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "s3"},
)
def raw_s3_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract S3 logs from sub-daily files and return one daily DataFrame."""
    return S3Extractor().extract(context)
