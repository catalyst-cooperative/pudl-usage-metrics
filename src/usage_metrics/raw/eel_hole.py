"""Extract data from PUDL Viewer (aka "eel hole") logs."""

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


class EelHoleExtractor(GCSExtractor):
    """Extractor for eel hole logs stored in GCS."""

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = "eel_hole_logs"
        self.bucket_name = "pudl-viewer-logs.catalyst.coop"
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
        day_start_date_str = context.partition_key
        partition_date = datetime.strptime(day_start_date_str, "%Y-%m-%d").strftime(
            "%Y/%m/%d"
        )
        file_name_prefix = f"run.googleapis.com/stdout/{partition_date}"
        blobs = [blob for blob in blobs if blob.name.startswith(file_name_prefix)]
        context.log.info(
            f"Extracting {len(blobs)} eel-hole logs for {context.partition_key}."
        )
        return blobs

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        return pd.read_json(file_path, lines=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "eel_hole"},
)
def raw_eel_hole_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract eel hole logs from sub-daily files and return one weekly DataFrame."""
    return EelHoleExtractor().extract(context)
