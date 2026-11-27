"""Extract data from PUDL Viewer (aka "eel hole") logs."""

from collections.abc import Iterable
from datetime import date
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Failure,
    asset,
)
from google.cloud import storage

from usage_metrics.paths import PUDL_METRICS_EEL_HOLE_LOGS_BUCKET
from usage_metrics.raw.extract import GCS_EXTRACT_RETRY_POLICY, GCSExtractor

FIRST_COMPLETE_RAW_DAY = date(2026, 5, 28)
"""The first day whose raw logs are all still in GCS.

Older logs expired under a retention rule that has since been relaxed, and 2026-05-27
only has part of its day. Outputs for earlier days can't be produced again.
"""


class EelHoleExtractor(GCSExtractor):
    """Extractor for eel hole logs stored in GCS."""

    concatenable_files = True
    require_files = True
    """The bucket's logs expire, so a backfill of an expired day must fail instead of
    writing an empty partition over its existing output."""

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = "eel_hole_logs"
        self.bucket_name = PUDL_METRICS_EEL_HOLE_LOGS_BUCKET
        super().__init__(*args, **kwargs)

    def get_blob_prefix(self, context: AssetExecutionContext) -> str:
        """Filter the bucket listing to this partition's date server-side."""
        partition_date = date.fromisoformat(context.partition_key).strftime("%Y/%m/%d")
        return f"run.googleapis.com/stdout/{partition_date}"

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
        day_start_date_str = context.partition_key
        partition_date = date.fromisoformat(day_start_date_str).strftime("%Y/%m/%d")
        file_name_prefix = f"run.googleapis.com/stdout/{partition_date}"
        filtered_blobs = [
            blob
            for blob in blobs
            if blob.name is not None and blob.name.startswith(file_name_prefix)
        ]
        context.log.info(
            f"Extracting {len(filtered_blobs)} eel-hole logs for {context.partition_key}."
        )
        return filtered_blobs

    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        return pd.read_json(file_path, lines=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "eel_hole"},
    retry_policy=GCS_EXTRACT_RETRY_POLICY,
)
def raw_eel_hole_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract eel hole logs from sub-daily files and return one weekly DataFrame.

    Raises:
        Failure: For a day before ``FIRST_COMPLETE_RAW_DAY``, so that nothing is made
            from what's left of its logs and written over its outputs.
    """
    day = date.fromisoformat(context.partition_key)
    if day < FIRST_COMPLETE_RAW_DAY:
        raise Failure(
            description=(
                f"Not processing {day}: its raw logs have expired, in whole or in "
                "part, so its existing outputs can't be produced again."
            ),
            allow_retries=False,
        )
    return EelHoleExtractor().extract(context)
