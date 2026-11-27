"""Where the ETL reads and writes data: local directories and GCS buckets.

Locations that depend on environment variables are functions, evaluated when called
rather than at import time, so that they can be changed after import (e.g. in tests).
"""

import os
from pathlib import Path

from platformdirs import user_cache_dir

# Where processed outputs go when METRICS_PROD_ENV is "prod". This default can be
# overridden with the environment variable of the same name.
PUDL_METRICS_GCS_BASE_PATH = "gs://metrics.catalyst.coop"

# GCS buckets, by bare name, that hold the raw usage logs and archives.
PUDL_METRICS_ARCHIVES_BUCKET = "pudl-usage-metrics-archives.catalyst.coop"
PUDL_METRICS_S3_LOGS_BUCKET = "pudl-s3-logs.catalyst.coop"
PUDL_METRICS_EEL_HOLE_LOGS_BUCKET = "pudl-viewer-logs.catalyst.coop"


def get_local_data_dir() -> Path:
    """Get the root of everything we store on the local machine.

    This applies whether the ETL runs in production or in local development. It is set
    by the ``PUDL_METRICS_LOCAL_DATA_DIR`` environment variable, and otherwise defaults
    to a per-user cache directory, so we never clutter the repository or the current
    working directory. In production the machine is ephemeral, so the location doesn't
    matter.
    """
    return (
        Path(
            os.environ.get("PUDL_METRICS_LOCAL_DATA_DIR")
            or user_cache_dir("pudl-usage-metrics")
        )
        .expanduser()
        .resolve()
    )


def get_raw_dir() -> Path:
    """Get the directory raw logs are downloaded to from the GCS buckets below.

    Each dataset has its own ``<dataset_name>/`` subdirectory. Files that are already
    present are not downloaded again.
    """
    return get_local_data_dir() / "raw"


def get_parquet_dir() -> Path:
    """Get the directory processed Parquet outputs go to when ``METRICS_PROD_ENV`` is "local"."""
    return get_local_data_dir() / "parquet"


def get_ipinfo_cache_dir() -> Path:
    """Get the directory of cached IPInfo geocoding results.

    The cache means we only pay for one lookup of each IP address.
    """
    return get_local_data_dir() / "ipinfo"


def get_gcs_base_path() -> str:
    """Get the GCS location processed outputs go to when ``METRICS_PROD_ENV`` is "prod".

    The default, ``PUDL_METRICS_GCS_BASE_PATH``, can be overridden by the environment
    variable of the same name. It must be a ``gs://`` URI, because anything else would
    be treated as a local path.
    """
    path = os.environ.get("PUDL_METRICS_GCS_BASE_PATH", PUDL_METRICS_GCS_BASE_PATH)
    if not path.startswith("gs://"):
        raise ValueError(
            f"PUDL_METRICS_GCS_BASE_PATH must be a gs:// URI, got {path!r}."
        )
    return path


def get_gcs_raw_path() -> str:
    """Get the GCS location of raw-layer artifacts, mirroring ``get_raw_dir()``.

    Like the processed outputs, it is under ``get_gcs_base_path()``, so pointing
    ``PUDL_METRICS_GCS_BASE_PATH`` at another bucket redirects these too.

    Unlike processed outputs, which are local unless ``METRICS_PROD_ENV`` is "prod",
    these artifacts are always in GCS. So that a development or test run can't change
    production data, any other run must set a location outside the production bucket.

    Raises:
        ValueError: If ``METRICS_PROD_ENV`` isn't "prod" and the location is in the
            production bucket.
    """
    base_path = get_gcs_base_path().rstrip("/")
    production_bucket = split_gcs_uri(PUDL_METRICS_GCS_BASE_PATH)[0]
    if (
        os.environ.get("METRICS_PROD_ENV", "local") != "prod"
        and split_gcs_uri(base_path)[0] == production_bucket
    ):
        raise ValueError(
            f"Only runs with METRICS_PROD_ENV=prod may use gs://{production_bucket}. "
            "Set PUDL_METRICS_GCS_BASE_PATH to a non-production gs:// location."
        )
    return f"{base_path}/raw"


def split_gcs_uri(uri: str) -> tuple[str, str]:
    """Split a ``gs://`` URI into its bucket name and object name."""
    bucket, _, object_name = uri.removeprefix("gs://").partition("/")
    return bucket, object_name
