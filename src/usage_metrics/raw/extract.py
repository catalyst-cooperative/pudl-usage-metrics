"""Generic extraction functionality for data from GCS."""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import (
    AssetExecutionContext,
)
from google.api_core.page_iterator import HTTPIterator
from google.cloud import storage
from tqdm import tqdm


class GCSExtractor(ABC):
    """Generic extractor base class for Google Cloud Storage logs."""

    def __init__(self, *args, **kwargs):
        """Create new extractor object and load metadata.

        Args:
            ds (datastore.Datastore): An initialized datastore, or subclass
        """
        if not self.dataset_name:
            raise NotImplementedError("self.dataset_name must be set.")
        if not self.bucket_name:
            raise NotImplementedError("self.bucket_name must be set.")

    @abstractmethod
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
        ...

    def get_blobs_from_gcs(
        self, blobs: list[storage.Blob], download_dir: Path
    ) -> list[Path]:
        """Download all selected blobs from GCS bucket.

        If the file already exists locally don't download it.
        """
        file_paths = []
        for blob in tqdm(blobs):
            file_name = blob.name.replace("/", "-")  # Replace folders with prefixes
            path_to_file = Path(download_dir, file_name)
            if not Path.exists(path_to_file):
                blob.download_to_filename(path_to_file)
                if Path.stat(path_to_file).st_size == 0:
                    # Handle download interruptions. #TODO: Less janky way to do this?
                    blob.download_to_filename(Path(download_dir, file_name))

            file_paths.append(Path(download_dir, file_name))
        return file_paths

    @abstractmethod
    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        ...

    def extract(self, context: AssetExecutionContext) -> pd.DataFrame:
        """Download all logs from GCS bucket.

        If the file already exists locally don't download it.
        """
        with TemporaryDirectory() as td:
            # Determine where to save these files
            if os.environ.get("DATA_DIR"):
                download_dir = Path(os.environ.get("DATA_DIR"), f"{self.dataset_name}/")
                if not Path.exists(download_dir):
                    Path.mkdir(download_dir)
            else:
                download_dir = td

            # Download logs from GCS
            bucket = storage.Client().get_bucket(self.bucket_name)
            blobs = bucket.list_blobs()
            blobs = self.filter_blobs(context, blobs)
            file_paths = self.get_blobs_from_gcs(blobs=blobs, download_dir=download_dir)

            # Extract logs into a Pandas DF
            weekly_dfs = []
            for path in file_paths:
                try:
                    weekly_dfs.append(self.load_file(path))
                except pd.errors.EmptyDataError:
                    context.log.warnings(f"{path} is an empty file, couldn't read.")
            if weekly_dfs:  # If data, return concatenated DF
                return pd.concat(weekly_dfs)
            return pd.DataFrame()
