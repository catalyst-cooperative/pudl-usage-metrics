"""Generic extraction functionality for data from GCS."""

import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import polars as pl
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

    concatenable_files: bool = False
    """Whether ``load_file`` can parse many source files concatenated into one.

    True for line-oriented formats (space-delimited logs, newline-delimited JSON),
    where combining the day's files and parsing once avoids >100k per-file parser
    invocations. False for whole-document JSON, where files must be parsed
    individually."""

    def __init__(self, *args, client: storage.Client | None = None, **kwargs):
        """Create new extractor object and load metadata.

        Args:
            client: A ``google.cloud.storage.Client`` to use for downloads. Left
                unset in production (one is created lazily on first use); inject
                a fake in tests.
        """
        if not self.dataset_name:
            raise NotImplementedError("self.dataset_name must be set.")
        if not self.bucket_name:
            raise NotImplementedError("self.bucket_name must be set.")
        self._client = client
        # Set in extract(); lets load_file() apply partition-specific handling.
        self.partition_key: str | None = None

    @property
    def gcs_client(self) -> storage.Client:
        """The GCS client, created on first use if one wasn't injected."""
        if self._client is None:
            self._client = storage.Client()
        return self._client

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
    def load_file(self, file_path: Path) -> pd.DataFrame | pl.DataFrame:
        """Read one source file (or a combined file) into a dataframe.

        May return either a pandas or a polars DataFrame; ``extract`` converts
        polars results to pandas before handing them downstream.
        """
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
        bucket = self.gcs_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.get_blob_prefix(context))
        blobs = self.filter_blobs(context, blobs)
        context.log.info(f"Downloading {len(blobs)} blobs from {self.bucket_name}.")
        return self.get_blobs_from_gcs(blobs=blobs, download_dir=download_dir)

    def combine_files(self, file_paths: list[Path], dest: Path) -> Path:
        """Concatenate downloaded files so a partition can be parsed in one pass.

        Reading >100k tiny files one at a time (a ``load_file`` call each) and
        then concatenating the results is dominated by per-call overhead. Writing
        them into a single file instead lets ``load_file`` parse the whole
        partition in one call. Files are separated by exactly one newline and
        empty files are skipped.
        """
        with dest.open("wb") as combined:
            for path in file_paths:
                data = path.read_bytes()
                if not data:
                    continue
                combined.write(data)
                if not data.endswith(b"\n"):
                    combined.write(b"\n")
        return dest

    def extract(self, context: AssetExecutionContext) -> pd.DataFrame:
        """Download the partition's logs from GCS and read them into one pandas DataFrame.

        Line-oriented sources (``concatenable_files``) are combined and parsed in
        a single pass; other sources are parsed one file at a time. ``load_file``
        may return polars frames, but the asset output is always pandas so
        downstream assets are unaffected. Blobs already present locally are not
        re-downloaded.
        """
        self.partition_key = context.partition_key
        download_dir = self.get_download_dir()
        file_paths = self.download_gcs_blobs(context, download_dir)

        if not file_paths:
            context.log.warning(f"No files found for {context.partition_key}.")
            return pd.DataFrame()

        if self.concatenable_files and len(file_paths) > 1:
            sources = [
                self.combine_files(
                    file_paths, download_dir / f"{context.partition_key}.combined"
                )
            ]
        else:
            sources = file_paths

        frames: list[pd.DataFrame] = []
        for source in sources:
            try:
                frame = self.load_file(source)
            except pd.errors.EmptyDataError, pl.exceptions.NoDataError:
                context.log.warning(f"{source} contains no data, skipping.")
                continue
            frames.append(
                frame.to_pandas() if isinstance(frame, pl.DataFrame) else frame
            )

        if not frames:
            context.log.warning(f"No data found for {context.partition_key}.")
            return pd.DataFrame()
        return frames[0] if len(frames) == 1 else pd.concat(frames)
