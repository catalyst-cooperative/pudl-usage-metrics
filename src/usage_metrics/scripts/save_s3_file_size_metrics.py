"""This script archives the sizes of each file in the S3 bucket."""

import logging
import sys
from datetime import date

import pandas as pd
import s3fs
from google.cloud import storage

logger = logging.getLogger()
logging.basicConfig(level="INFO")


def get_s3_bucket_objects_detailed(bucket_name: str) -> pd.DataFrame:
    """Return a dataframe with all objects in an S3 bucket with their sizes recursively."""
    fs = s3fs.S3FileSystem(anon=True)

    # Use glob to get all files recursively
    all_files = fs.glob(f"s3://{bucket_name}/**", detail=True)

    # Convert to list of dictionaries for DataFrame creation
    objects = []
    for path, info in all_files.items():
        if info["type"] == "file":  # Only include actual files, not directories
            objects.append(
                {
                    "name": path,
                    "size": info["size"],
                }
            )

    df = pd.DataFrame(objects)

    if df.empty:
        return df

    # Extract object name without bucket prefix
    df["object_name"] = df["name"].str.replace(f"{bucket_name}/", "", 1)

    return (
        df[["object_name", "size"]]
        .rename(columns={"size": "size_bytes"})
        .reset_index(drop=True)
    )


def upload_to_bucket(df):
    """Upload a CSV to a Google Cloud object."""
    bucket_name = "pudl-usage-metrics-archives.catalyst.coop"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = f"s3-file-size/{date.today().strftime('%Y-%m-%d')}.csv"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_csv(index=False), "text/csv")

    logger.info(f"Uploaded today's data to {blob_name}.")


def save_s3_file_sizes():
    """Archive all S3 file sizes in the pudl.catalyst.coop bucket to GCS."""
    bucket_name = "pudl.catalyst.coop"
    df = get_s3_bucket_objects_detailed(bucket_name)
    upload_to_bucket(df)


if __name__ == "__main__":
    sys.exit(save_s3_file_sizes())
