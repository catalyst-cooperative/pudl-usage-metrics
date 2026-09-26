"""Test where the ETL stores data on the local machine."""

from pathlib import Path

import pandas as pd
import pytest

from usage_metrics import paths
from usage_metrics.raw.extract import GCSExtractor


def test_local_data_dir_from_env(tmp_path, monkeypatch):
    """All local locations are subdirectories of the directory in the env var."""
    root = tmp_path / "data"
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(root))
    assert paths.get_local_data_dir() == root.resolve()
    assert paths.get_raw_dir() == root.resolve() / "raw"
    assert paths.get_parquet_dir() == root.resolve() / "parquet"
    assert paths.get_ipinfo_cache_dir() == root.resolve() / "ipinfo"


def test_local_data_dir_expands_user_and_relative_paths(tmp_path, monkeypatch):
    """The env var may contain ~ or be relative; the result is absolute."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", "~/data")
    assert paths.get_local_data_dir() == (tmp_path / "data").resolve()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", "rel")
    assert paths.get_local_data_dir() == (tmp_path / "rel").resolve()


@pytest.mark.parametrize("value", [None, ""])
def test_local_data_dir_default_is_user_cache(tmp_path, monkeypatch, value):
    """Unset (or empty), the root is a per-user cache dir, not the working directory."""
    home, cwd = tmp_path / "home", tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.chdir(cwd)
    if value is None:
        monkeypatch.delenv("PUDL_METRICS_LOCAL_DATA_DIR", raising=False)
    else:
        monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", value)

    root = paths.get_local_data_dir()
    assert root.name == "pudl-usage-metrics"
    assert home.resolve() in root.parents
    assert list(cwd.iterdir()) == []


def test_extractor_download_dir(tmp_path, monkeypatch):
    """Extractors download into <raw dir>/<dataset_name>, created on demand."""
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(tmp_path))

    class _Extractor(GCSExtractor):
        dataset_name = "some_logs"
        bucket_name = "some-bucket"

        def filter_blobs(self, context, blobs):
            return list(blobs)

        def load_file(self, file_path):
            return pd.DataFrame()

    download_dir = _Extractor().get_download_dir()
    assert download_dir == tmp_path.resolve() / "raw" / "some_logs"
    assert download_dir.is_dir()
    assert _Extractor().get_download_dir() == download_dir  # idempotent


def test_geocode_ip_cache_follows_env(tmp_path, monkeypatch):
    """The IPInfo cache is created under the current data dir, and is hit on reuse."""
    from usage_metrics import helpers

    calls = []

    def fake_api_call(ip_address):
        calls.append(ip_address)
        return {"ip": ip_address}

    monkeypatch.setattr(helpers, "_geocode_ip", fake_api_call)
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(tmp_path))
    assert helpers.geocode_ip("1.2.3.4") == {"ip": "1.2.3.4"}
    assert helpers.geocode_ip("1.2.3.4") == {"ip": "1.2.3.4"}
    assert calls == ["1.2.3.4"]
    assert any(Path(tmp_path, "ipinfo").rglob("*"))


def test_gcs_base_path(monkeypatch):
    """The GCS output path has a default, can be overridden, and must be a gs:// URI."""
    monkeypatch.delenv("PUDL_METRICS_GCS_BASE_PATH", raising=False)
    assert paths.get_gcs_base_path() == "gs://metrics.catalyst.coop"
    monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", "gs://other-bucket/prefix")
    assert paths.get_gcs_base_path() == "gs://other-bucket/prefix"
    monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", "test.catalyst.coop")
    with pytest.raises(ValueError, match="gs://"):
        paths.get_gcs_base_path()
