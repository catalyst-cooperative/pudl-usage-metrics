"""Test where the ETL stores data on the local machine."""

import os
from contextlib import chdir

import pandas as pd
import pytest

from usage_metrics import paths
from usage_metrics.raw.extract import GCSExtractor


def test_local_data_dir_from_env(tmp_path, mocker):
    """All local locations are subdirectories of the directory in the env var."""
    root = tmp_path / "data"
    mocker.patch.dict(os.environ, {"PUDL_METRICS_LOCAL_DATA_DIR": str(root)})
    assert paths.get_local_data_dir() == root.resolve()
    assert paths.get_raw_dir() == root.resolve() / "raw"
    assert paths.get_parquet_dir() == root.resolve() / "parquet"
    assert paths.get_ipinfo_cache_dir() == root.resolve() / "ipinfo"


def test_local_data_dir_expands_user_and_relative_paths(tmp_path, mocker):
    """The env var may contain ~ or be relative; the result is absolute."""
    mocker.patch.dict(
        os.environ,
        {"HOME": str(tmp_path), "PUDL_METRICS_LOCAL_DATA_DIR": "~/data"},
    )
    assert paths.get_local_data_dir() == (tmp_path / "data").resolve()
    os.environ["PUDL_METRICS_LOCAL_DATA_DIR"] = "rel"
    with chdir(tmp_path):
        assert paths.get_local_data_dir() == (tmp_path / "rel").resolve()


@pytest.mark.parametrize("value", [None, ""])
def test_local_data_dir_default_is_user_cache(tmp_path, mocker, value):
    """Unset (or empty), the root is a per-user cache dir, not the working directory."""
    home, cwd = tmp_path / "home", tmp_path / "cwd"
    cwd.mkdir()
    # patch.dict restores the whole environment, including what's removed after it.
    mocker.patch.dict(os.environ, {"HOME": str(home)})
    os.environ.pop("XDG_CACHE_HOME", None)
    if value is None:
        os.environ.pop("PUDL_METRICS_LOCAL_DATA_DIR", None)
    else:
        os.environ["PUDL_METRICS_LOCAL_DATA_DIR"] = value

    with chdir(cwd):
        root = paths.get_local_data_dir()
    assert root.name == "pudl-usage-metrics"
    assert home.resolve() in root.parents
    assert list(cwd.iterdir()) == []


def test_extractor_download_dir(data_dir):
    """Extractors download into <raw dir>/<dataset_name>, created on demand."""

    class _Extractor(GCSExtractor):
        dataset_name = "some_logs"
        bucket_name = "some-bucket"

        def filter_blobs(self, context, blobs):
            return list(blobs)

        def load_file(self, file_path):
            return pd.DataFrame()

    download_dir = _Extractor().get_download_dir()
    assert download_dir == data_dir.resolve() / "raw" / "some_logs"
    assert download_dir.is_dir()
    assert _Extractor().get_download_dir() == download_dir  # idempotent


def test_gcs_base_path(mocker):
    """The GCS output path has a default, can be overridden, and must be a gs:// URI."""
    mocker.patch.dict(os.environ)
    os.environ.pop("PUDL_METRICS_GCS_BASE_PATH", None)
    assert paths.get_gcs_base_path() == "gs://metrics.catalyst.coop"
    os.environ["PUDL_METRICS_GCS_BASE_PATH"] = "gs://other-bucket/prefix"
    assert paths.get_gcs_base_path() == "gs://other-bucket/prefix"
    os.environ["PUDL_METRICS_GCS_BASE_PATH"] = "test.catalyst.coop"
    with pytest.raises(ValueError, match="gs://"):
        paths.get_gcs_base_path()
