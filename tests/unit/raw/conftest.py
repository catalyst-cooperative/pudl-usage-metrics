"""Fixtures for testing GCS extractors.

The fake GCS they run against is in ``tests/conftest.py``.
"""

from pathlib import Path

import pytest
from dagster import build_asset_context

DATA_DIR = Path(__file__).parents[2] / "data"


@pytest.fixture
def download_dir(tmp_path, monkeypatch):
    """Point extractors at a temp download directory via ``PUDL_METRICS_LOCAL_DATA_DIR``."""
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def partition_context():
    """Return a factory for a Dagster asset context with a partition key."""
    return lambda partition_key: build_asset_context(partition_key=partition_key)


@pytest.fixture
def run_extract(monkeypatch):
    """Run ``extractor.extract`` with the download step stubbed to ``paths``."""

    def _run(extractor, context, paths):
        monkeypatch.setattr(extractor, "download_gcs_blobs", lambda *a, **k: paths)
        return extractor.extract(context)

    return _run


@pytest.fixture
def s3_fixture_blobs():
    """Return a factory: subdir name -> ``{blob_name: bytes}`` from tests/data/s3."""

    def _load(subdir: str) -> dict[str, bytes]:
        return {
            path.name: path.read_bytes()
            for path in sorted((DATA_DIR / "s3" / subdir).iterdir())
            if path.is_file()
        }

    return _load


@pytest.fixture
def filtered_blob_names(fake_gcs, partition_context):
    """Return a function listing the blob names an extractor keeps for a partition.

    Builds the extractor with a fake bucket holding empty blobs of the given names,
    so tests only need to give the names and the expected survivors.
    """

    def _filter(extractor_cls, partition_key, blob_names, **kwargs) -> list[str]:
        bucket_name = extractor_cls(**kwargs).bucket_name
        client = fake_gcs({bucket_name: dict.fromkeys(blob_names, b"")})
        extractor = extractor_cls(client=client, **kwargs)
        listed = client.bucket(bucket_name).list_blobs()
        kept = extractor.filter_blobs(partition_context(partition_key), listed)
        return [blob.name for blob in kept]

    return _filter
