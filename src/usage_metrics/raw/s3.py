"""Extract data from S3 logs."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)
from google.cloud import storage
from tqdm import tqdm

BUCKET_URI = "pudl-s3-logs.catalyst.coop"
PATH_EXT = "data/pudl_s3_logs/"

s3_weekly_partitions = WeeklyPartitionsDefinition(start_date="2023-08-16")


def download_s3_logs_from_gcs(
    context: AssetExecutionContext, partition_dates: tuple[str], download_dir: Path
) -> list[Path]:
    """Download all logs from GCS bucket.

    If the file already exists locally don't download it.
    """
    bucket = storage.Client().get_bucket(BUCKET_URI)
    blobs = bucket.list_blobs()
    blobs = [blob for blob in blobs if blob.name.startswith(partition_dates)]
    file_paths = []
    for blob in tqdm(blobs):
        path_to_file = Path(download_dir, blob.name)
        if not Path.exists(path_to_file):
            blob.download_to_filename(path_to_file)
            if Path.stat(path_to_file).st_size == 0:
                # Handle download interruptions. #TODO: Less janky way to do this?
                blob.download_to_filename(Path(download_dir, blob.name))

        file_paths.append(Path(download_dir, blob.name))
    return file_paths


@asset(
    partitions_def=s3_weekly_partitions,
    tags={"source": "s3"},
)
def raw_s3_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract S3 logs from sub-daily files and return one daily DataFrame."""
    week_start_date_str = context.partition_key
    week_date_range = pd.date_range(start=week_start_date_str, periods=7, freq="D")

    weekly_dfs = []

    with TemporaryDirectory() as td:
        # Determine where to save these files
        if os.environ.get("DATA_DIR"):
            download_dir = Path(os.environ.get("DATA_DIR"), "pudl_s3_logs/")
            if not Path.exists(download_dir):
                Path.mkdir(download_dir)
        else:
            download_dir = td
        context.log.info(f"Saving S3 logs to {download_dir}.")

        # Get all logs in a week
        file_paths = download_s3_logs_from_gcs(
            context=context,
            partition_dates=tuple(week_date_range.strftime("%Y-%m-%d")),
            download_dir=download_dir,
        )

        for path in file_paths:
            try:
                weekly_dfs.append(pd.read_csv(path, delimiter=" ", header=None))
            except pd.errors.EmptyDataError:
                context.log.warnings(f"{path} is an empty file, couldn't read.")
        if weekly_dfs:  # If data
            return pd.concat(weekly_dfs)
        return pd.DataFrame()
