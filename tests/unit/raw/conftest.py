"""Fakes and fixtures for testing GCS extractors without touching GCS."""

from pathlib import Path

import pytest
from dagster import build_asset_context

from usage_metrics.raw import extract

DATA_DIR = Path(__file__).parents[2] / "data"


class FakeBlob:
    """Stand-in for ``google.cloud.storage.Blob`` backed by in-memory bytes."""

    def __init__(self, name: str, data: bytes, bucket: object = None):
        """Store the blob name and contents."""
        self.name = name
        self._data = data
        self.bucket = bucket

    def download_to_filename(self, filename) -> None:
        """Write the blob contents to a local path."""
        Path(filename).write_bytes(self._data)


class FakeBucket:
    """Stand-in for ``google.cloud.storage.Bucket``."""

    def __init__(self, blobs: dict[str, bytes]):
        """Build fake blobs from a ``{name: bytes}`` mapping."""
        self._blobs = {name: FakeBlob(name, data, self) for name, data in blobs.items()}

    def list_blobs(self, prefix: str | None = None) -> list[FakeBlob]:
        """Return blobs (name-sorted, like GCS) whose name matches ``prefix``."""
        return [
            blob
            for name, blob in sorted(self._blobs.items())
            if prefix is None or name.startswith(prefix)
        ]


class FakeClient:
    """Stand-in for ``google.cloud.storage.Client``."""

    def __init__(self, buckets: dict[str, dict[str, bytes]]):
        """Build fake buckets from a ``{bucket_name: {blob_name: bytes}}`` mapping."""
        self._buckets = {name: FakeBucket(blobs) for name, blobs in buckets.items()}

    def bucket(self, name: str) -> FakeBucket:
        """Return the named fake bucket."""
        return self._buckets[name]


def _fake_download_many(
    blob_file_pairs,
    *,
    skip_if_exists: bool = False,
    raise_exception: bool = False,
    **_kwargs,
):
    """In-process stand-in for ``transfer_manager.download_many``."""
    results = []
    for blob, path in blob_file_pairs:
        path = Path(path)
        if skip_if_exists and path.exists():
            results.append(None)
            continue
        try:
            blob.download_to_filename(path)
            results.append(None)
        except Exception as err:
            if raise_exception:
                raise
            results.append(err)
    return results


@pytest.fixture
def patch_download_many(monkeypatch):
    """Replace the parallel downloader with a synchronous in-process copy."""
    monkeypatch.setattr(extract.transfer_manager, "download_many", _fake_download_many)


@pytest.fixture
def make_client():
    """Return a factory for a ``FakeClient`` from ``{bucket: {blob: bytes}}``."""
    return FakeClient


@pytest.fixture
def download_dir(tmp_path, monkeypatch):
    """Point extractors at a temp download directory via ``DATA_DIR``."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def partition_context():
    """Return a factory for a Dagster asset context with a partition key."""
    return lambda partition_key: build_asset_context(partition_key=partition_key)


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
