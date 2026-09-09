"""Generic extraction functionality for data from GCS."""

import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Backoff,
    Jitter,
    RetryPolicy,
)
from google.api_core.page_iterator import HTTPIterator
from google.cloud import storage
from google.cloud.storage import transfer_manager

GCS_EXTRACT_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)
"""Retry the whole extract step if it fails.

Downloads and reads of many small blobs occasionally fail for transient reasons
that the client library's per-request retries don't cover. Retrying the step is
cheap because already-downloaded blobs are skipped, and it keeps a single bad day
from leaving a permanent gap in the partitioned output."""

MAX_DOWNLOAD_WORKERS = int(os.environ.get("GCS_DOWNLOAD_WORKERS", "64"))
"""Number of worker processes used to download blobs from GCS concurrently.

Downloading many small blobs is latency-bound, not CPU-bound: each worker spends
almost all its time waiting on the network. A shared ``requests`` connection pool
caps a single process at ~10 concurrent transfers, so ``transfer_manager`` spreads
the work across processes (each with its own pool) rather than threads.

Useful concurrency is limited by per-object round-trip latency and GCS-side
throughput (roughly 50-150 before returns diminish sharply), not by the runner's
vCPU count, so this is a fixed default rather than a function of ``os.cpu_count``.
Override with the ``GCS_DOWNLOAD_WORKERS`` env var to tune for a specific runner."""


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
        """Download all selected blobs from GCS bucket in parallel.

        Blobs whose local file already exists are skipped. Folder separators in
        blob names are flattened to hyphens so every file lands in a single dir.
        Downloading serially is the dominant cost for sources with many small
        files (S3 logs can be >100k blobs per day), so downloads are spread
        across a process pool.

        Each download validates the object checksum and retries transient
        failures (``download_to_filename`` defaults), and ``raise_exception``
        surfaces any download that still fails so the run doesn't silently
        proceed with missing data.
        """
        file_paths = [Path(download_dir, blob.name.replace("/", "-")) for blob in blobs]
        transfer_manager.download_many(
            list(zip(blobs, file_paths, strict=True)),
            skip_if_exists=True,
            raise_exception=True,
            worker_type=transfer_manager.PROCESS,
            max_workers=MAX_DOWNLOAD_WORKERS,
        )
        return file_paths

    @abstractmethod
    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Read in file as dataframe."""
        ...

    def get_download_dir(self) -> Path:
        """Get download directory as path."""
        # Determine where to save these files
        if os.environ.get("DATA_DIR"):
            download_dir = Path(os.environ.get("DATA_DIR"), f"{self.dataset_name}/")
            if not Path.exists(download_dir):
                Path.mkdir(download_dir, parents=True, exist_ok=True)
        else:
            td = tempfile.mkdtemp()
            download_dir = Path(td)
        return download_dir

    def get_blob_prefix(self, context: AssetExecutionContext) -> str | None:
        """Return a blob name prefix to filter the bucket listing server-side.

        Passing a prefix to ``list_blobs`` avoids enumerating every object in the
        bucket on every partition, which otherwise dominates extraction time and
        grows without bound as the bucket accumulates logs. Subclasses whose
        relevant files share a common name prefix (e.g. a partition date) should
        override this. ``filter_blobs`` is still applied to the narrowed listing.
        """
        return None

    def download_gcs_blobs(
        self, context: AssetExecutionContext, download_dir: Path
    ) -> list[Path]:
        """Download GCS blobs and return paths to files."""
        # Download logs from GCS
        bucket = storage.Client().bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.get_blob_prefix(context))
        blobs = self.filter_blobs(context, blobs)
        context.log.info(f"Downloading {len(blobs)} blobs from {self.bucket_name}.")
        return self.get_blobs_from_gcs(blobs=blobs, download_dir=download_dir)

    def extract_logs_into_list(
        self, context: AssetExecutionContext, file_paths: list[Path]
    ) -> list[pd.DataFrame]:
        """Read files into a list of Pandas DataFrames."""
        list_dfs = []
        for path in file_paths:
            try:
                list_dfs.append(self.load_file(path))
            except pd.errors.EmptyDataError:
                context.log.warning(f"{path} is an empty file, couldn't read.")
        return list_dfs

    def extract(self, context: AssetExecutionContext) -> pd.DataFrame:
        """Download all logs from GCS bucket.

        If the file already exists locally don't download it.
        """
        download_dir = self.get_download_dir()
        file_paths = self.download_gcs_blobs(context, download_dir)
        list_dfs = self.extract_logs_into_list(context, file_paths)

        df = pd.DataFrame()
        if list_dfs:  # If data, return concatenated DF
            df = pd.concat(list_dfs)
        return df
