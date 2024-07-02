"""Script for downloading all s3://pudl.catalyst.coop logs from a GCS bucket."""

import os

from google.cloud import storage
from tqdm import tqdm


def download_logs_from_gcs(bucket_uri: str, local_dir: str):
    """
    Download all logs from GCS bucket.

    If the file already exists locally don't download it.
    """
    bucket = storage.Client().get_bucket(bucket_uri)
    blobs = bucket.list_blobs()
    for blob in tqdm(blobs):
        if not os.path.exists(os.path.join(local_dir, blob.name)):
            blob.download_to_filename(os.path.join(local_dir, blob.name))
        else:
            print(f"File {blob.name} already exists locally. Skipping download.")


if __name__ == "__main__":
    bucket_uri = "pudl-s3-logs.catalyst.coop"
    local_dir = "data/pudl_s3_logs/"
    download_logs_from_gcs(bucket_uri, local_dir)
