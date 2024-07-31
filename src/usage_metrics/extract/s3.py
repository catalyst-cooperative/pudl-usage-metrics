"""Extract data from S3 logs."""

import logging
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition, Definitions, asset
from google.cloud import storage
from tqdm import tqdm

BUCKET_URI = "pudl-s3-logs.catalyst.coop"

LOCAL_DIR = "data/pudl_s3_logs/"
if not Path.exists(Path(LOCAL_DIR)):
    Path.mkdir(LOCAL_DIR)

logger = logging.getLogger()


def download_s3_logs_from_gcs(
    partition_date_str: str,
) -> list[Path]:
    """Download all logs from GCS bucket.

    If the file already exists locally don't download it.
    """
    bucket = storage.Client().get_bucket(BUCKET_URI)
    blobs = bucket.list_blobs()
    blobs = [blob for blob in blobs if blob.name.startswith(partition_date_str)]
    file_paths = []
    for blob in tqdm(blobs):
        if not Path.exists(Path(LOCAL_DIR, blob.name)):
            blob.download_to_filename(Path(LOCAL_DIR, blob.name))
        else:
            logging.info(f"File {blob.name} already exists locally. Skipping download.")
        file_paths.append(Path(LOCAL_DIR, blob.name))
        logger.info(file_paths)
    return file_paths


@asset(partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"))
def extract_s3_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract S3 logs from sub-daily files and return one daily DataFrame."""
    partition_date_str = context.partition_key
    file_paths = download_s3_logs_from_gcs(partition_date_str)
    daily_dfs = []
    for path in file_paths:
        logger.info(path)
        daily_dfs.append(pd.read_csv(path, delimiter=" ", header=None))
    return pd.concat(daily_dfs)


defs = Definitions(
    assets=[extract_s3_logs],
)
