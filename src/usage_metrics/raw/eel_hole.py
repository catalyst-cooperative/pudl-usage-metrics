"""Extract data from PUDL Viewer (aka "eel hole") logs."""

from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Output,
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

TRANSFER_DELAY = timedelta(days=1, hours=2)
"""How long after the start of a partition's day its logs are all in GCS."""


def _utcnow() -> datetime:
    return datetime.now(UTC)


class EelHoleExtractor(GCSExtractor):
    """Extractor for eel hole logs stored in GCS."""

    concatenable_files = True

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
    output_required=False,
)
def raw_eel_hole_logs(
    context: AssetExecutionContext,
) -> Iterator[Output[pd.DataFrame]]:
    """Extract one day of eel hole logs from its sub-daily files.

    Nothing is produced for a day whose logs have expired, or aren't all in GCS yet:
    processing part of a day could replace its complete outputs.
    """
    day = date.fromisoformat(context.partition_key)
    if day < FIRST_COMPLETE_RAW_DAY:
        context.log.info(f"Skipping {day}: its raw logs have expired.")
        return
    ready = datetime.combine(day, time(0), UTC) + TRANSFER_DELAY
    if _utcnow() < ready:
        context.log.info(f"Skipping {day}: its logs aren't due until {ready}.")
        return
    yield from EelHoleExtractor().extract_partition(context)
