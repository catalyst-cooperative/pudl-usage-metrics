"""Extract data from S3 logs."""

from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)
from google.cloud import storage
from tqdm import tqdm

BUCKET_URI = "pudl-s3-logs.catalyst.coop"
LOCAL_DIR = "data/pudl_s3_logs/"
# if not Path.exists(Path(LOCAL_DIR)):
#     Path.mkdir(LOCAL_DIR)


def download_s3_logs_from_gcs(
    partition_dates: tuple[str],
) -> list[Path]:
    """Download all logs from GCS bucket.

    If the file already exists locally don't download it.
    """
    bucket = storage.Client().get_bucket(BUCKET_URI)
    blobs = bucket.list_blobs()
    blobs = [blob for blob in blobs if blob.name.startswith(partition_dates)]
    file_paths = []
    for blob in tqdm(blobs):
        if not Path.exists(Path(LOCAL_DIR, blob.name)):
            blob.download_to_filename(Path(LOCAL_DIR, blob.name))
        file_paths.append(Path(LOCAL_DIR, blob.name))
    return file_paths


@asset(partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"))
def extract_s3_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract S3 logs from sub-daily files and return one daily DataFrame."""
    week_start_date_str = context.partition_key
    week_date_range = pd.date_range(start=week_start_date_str, periods=7, freq="D")

    weekly_dfs = []
    file_paths = download_s3_logs_from_gcs(
        tuple(week_date_range.strftime("%Y-%m-%d")),
    )  # Get all logs in a day
    for path in file_paths:
        weekly_dfs.append(pd.read_csv(path, delimiter=" ", header=None))
    return pd.concat(weekly_dfs)
