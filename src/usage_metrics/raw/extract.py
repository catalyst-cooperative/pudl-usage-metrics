"""Generic extraction functionality for data from GCS."""

import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from dagster import (
    AssetExecutionContext,
    Backoff,
    Jitter,
    RetryPolicy,
)
from google.cloud import storage
from google.cloud.storage import transfer_manager

from usage_metrics.paths import get_raw_dir

GCS_EXTRACT_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)
"""Retry the whole extract step if it fails.

Retrying the step is cheap because already-downloaded blobs are skipped. Adding a
jitter avoids many workers or partitions retrying at the same time if there's a
transient outage.
"""

DEFAULT_DOWNLOAD_WORKERS = 32
"""Default number of threads used to download blobs from GCS concurrently."""

DEFAULT_COMPOSE_WORKERS = 128
"""Default number of threads issuing server-side ``compose()`` calls concurrently.

Compaction reduces hundreds of thousands of tiny objects to a few larger objects using
thousands of GCS server-side ``compose()`` calls. This constant controls how many of
those calls can be running in parallel. It is a separate setting from
``DEFAULT_DOWNLOAD_WORKERS`` because object composition is not bounded by our VM
resources, and is by far the slowest step in the log compaction. It can be overridden
with an explicit compose_workers argument, or globally via the ``GCS_COMPOSE_WORKERS``
env var.
"""


class GCSExtractor(ABC):
    """Generic extractor base class for Google Cloud Storage logs."""

    dataset_name: str
    """Name of the dataset, set by subclasses before calling ``super().__init__()``."""

    bucket_name: str
    """Name of the GCS bucket to extract from, set by subclasses before calling
    ``super().__init__()``."""

    concatenable_files: bool = False
    """Whether ``load_file`` can parse many source files concatenated into one.

    True for line-oriented formats (space-delimited logs, newline-delimited JSON),
    where combining the day's files and parsing once avoids >100k per-file parser
    invocations. False for whole-document JSON, where files must be parsed
    individually."""

    def __init__(
        self,
        *args,
        client: storage.Client | None = None,
        download_workers: int | None = None,
        compose_workers: int | None = None,
        **kwargs,
    ):
        """Create new extractor object and load metadata.

        Args:
            client: A ``google.cloud.storage.Client`` to use for downloads. Left
                unset in production (one is created lazily on first use); inject
                a fake in tests.
            download_workers: Number of concurrent blob-download threads. Defaults
                to the ``GCS_DOWNLOAD_WORKERS`` env var, then
                ``DEFAULT_DOWNLOAD_WORKERS``.
            compose_workers: Number of concurrent server-side ``compose()``
                threads. Defaults to the ``GCS_COMPOSE_WORKERS`` env var, then
                ``DEFAULT_COMPOSE_WORKERS``.
        """
        if not self.dataset_name:
            raise NotImplementedError("self.dataset_name must be set.")
        if not self.bucket_name:
            raise NotImplementedError("self.bucket_name must be set.")
        self._client = client
        self.download_workers = download_workers or int(
            os.environ.get("GCS_DOWNLOAD_WORKERS", DEFAULT_DOWNLOAD_WORKERS)
        )
        self.compose_workers = compose_workers or int(
            os.environ.get("GCS_COMPOSE_WORKERS", DEFAULT_COMPOSE_WORKERS)
        )
        # Set in extract(); lets load_file() apply partition-specific handling.
        self.partition_key: str | None = None
        # Number of source objects behind the last download, when a subclass
        # reduces them before downloading (so it differs from the file count).
        self.source_object_count: int | None = None

    @property
    def gcs_client(self) -> storage.Client:
        """The GCS client, created on first use if one wasn't injected.

        The HTTP connection pool is sized to the larger of ``download_workers``
        and ``compose_workers`` so the concurrent threads don't contend on the
        default 10-connection pool.
        """
        if self._client is None:
            client = storage.Client()
            pool_size = max(self.download_workers, self.compose_workers)
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=pool_size,
                pool_maxsize=pool_size,
            )
            client._http.mount("https://", adapter)
            self._client = client
        return self._client

    @abstractmethod
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
        ...

    def get_blobs_from_gcs(
        self, blobs: list[storage.Blob], download_dir: Path
    ) -> list[Path]:
        """Download all selected blobs from GCS bucket in parallel.

        Blobs whose local file already exists are skipped. Folder separators in
        blob names are flattened to hyphens so every file lands in a single dir.
        Downloading serially is the dominant cost for sources with many small
        files (S3 logs can be >100k blobs per day), so downloads are spread
        across a thread pool.

        Each download validates the object checksum and retries transient
        failures (``download_to_filename`` defaults), and ``raise_exception``
        surfaces any download that still fails so the run doesn't silently
        proceed with missing data.
        """
        file_paths = []
        for blob in blobs:
            assert blob.name is not None, f"Blob {blob} has no name."
            file_paths.append(Path(download_dir, blob.name.replace("/", "-")))
        transfer_manager.download_many(
            [(blob, str(path)) for blob, path in zip(blobs, file_paths, strict=True)],
            skip_if_exists=True,
            raise_exception=True,
            worker_type=transfer_manager.THREAD,
            max_workers=self.download_workers,
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
        """Get the local directory raw files for this dataset are downloaded to."""
        download_dir = get_raw_dir() / self.dataset_name
        download_dir.mkdir(parents=True, exist_ok=True)
        return download_dir

    def get_blob_prefix(self, context: AssetExecutionContext) -> str | None:
        """Return a blob name prefix to filter the bucket listing server-side.

        Passing a prefix to ``list_blobs`` avoids enumerating every object in the bucket
        on every partition. Subclasses whose files share a common name prefix (e.g. a
        partition date) should override this.
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

    def build_combined_file(
        self, context: AssetExecutionContext
    ) -> tuple[Path, int] | None:
        """Download the partition's blobs and concatenate them into one file.

        This is ``extract`` minus the parsing, for callers that want the raw
        combined bytes (e.g. to compact them into a single artifact).

        Returns:
            ``(combined file, number of source objects)``, or ``None`` when the
            partition has no files.
        """
        self.partition_key = (
            context.partition_key if context.has_partition_key else None
        )
        label = self.partition_key or self.dataset_name
        download_dir = self.get_download_dir()
        self.source_object_count = None
        file_paths = self.download_gcs_blobs(context, download_dir)
        if not file_paths:
            context.log.warning(f"No files found for {label}.")
            return None
        combined = self.combine_files(file_paths, download_dir / f"{label}.combined")
        count = (
            self.source_object_count
            if self.source_object_count is not None
            else len(file_paths)
        )
        return combined, count

    def extract(self, context: AssetExecutionContext) -> pd.DataFrame:
        """Download the partition's logs from GCS and read them into one pandas DataFrame.

        Line-oriented sources (``concatenable_files``) are combined and parsed in
        a single pass; other sources are parsed one file at a time. ``load_file``
        may return polars frames, but the asset output is always pandas so
        downstream assets are unaffected. Blobs already present locally are not
        re-downloaded.
        """
        # Non-partitioned assets (e.g. the cumulative GitHub metrics) also use
        # this method, so partition_key may be unset.
        self.partition_key = (
            context.partition_key if context.has_partition_key else None
        )
        label = self.partition_key or self.dataset_name
        download_dir = self.get_download_dir()
        file_paths = self.download_gcs_blobs(context, download_dir)

        if not file_paths:
            context.log.warning(f"No files found for {label}.")
            return pd.DataFrame()

        if self.concatenable_files and len(file_paths) > 1:
            sources = [
                self.combine_files(file_paths, download_dir / f"{label}.combined")
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
            context.log.warning(f"No data found for {label}.")
            return pd.DataFrame()
        return frames[0] if len(frames) == 1 else pd.concat(frames)
