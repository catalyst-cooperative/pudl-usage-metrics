"""Fakes and fixtures for testing GCS extractors without touching GCS.

The fake classes are plain importable names (``from .conftest import FakeBlob``) for
tests that need to build blobs directly."""

from pathlib import Path

import pytest
from dagster import build_asset_context
from google.api_core.exceptions import BadRequest, PreconditionFailed

from usage_metrics.raw import extract

DATA_DIR = Path(__file__).parents[2] / "data"


MAX_COMPOSE_SOURCES = 32
MAX_COMPONENTS = 1024


class FakeBlob:
    """Stand-in for ``google.cloud.storage.Blob`` backed by in-memory bytes."""

    def __init__(
        self,
        name: str,
        data: bytes = b"",
        bucket: FakeBucket | None = None,
        time_created=None,
        metadata: dict[str, str] | None = None,
        components: int = 1,
    ):
        """Store the blob name and contents."""
        self.name = name
        self._data = data
        self.bucket = bucket
        self.time_created = time_created
        self.metadata = metadata
        self.components = components
        self.content_type: str | None = None
        self.generation: int | None = 1

    @property
    def size(self) -> int:
        """Size of the blob in bytes."""
        return len(self._data)

    def download_to_filename(self, filename) -> None:
        """Write the blob contents to a local path."""
        Path(filename).write_bytes(self._data)

    def _store(self) -> None:
        assert self.bucket is not None
        self.bucket._blobs[self.name] = self

    def upload_from_filename(self, filename, content_type=None) -> None:
        """Read a local file into the blob and store it in its bucket."""
        self._data = Path(filename).read_bytes()
        self.content_type = content_type
        self._store()

    def compose(self, sources, if_generation_match=None, **_kwargs) -> None:
        """Concatenate ``sources`` into this blob, enforcing GCS's limits."""
        assert self.bucket is not None
        if not 1 <= len(sources) <= MAX_COMPOSE_SOURCES:
            raise ValueError(f"Can compose 1-{MAX_COMPOSE_SOURCES} sources.")
        if if_generation_match == 0 and self.name in self.bucket._blobs:
            raise PreconditionFailed(f"{self.name} already exists.")
        components = sum(s.components for s in sources)
        if components > MAX_COMPONENTS:
            raise BadRequest(f"Composite would have {components} components.")
        self._data = b"".join(s._data for s in sources)
        self.components = components
        self._store()
        self.bucket.compose_calls.append(self.name)


class FakeBucket:
    """Stand-in for ``google.cloud.storage.Bucket``."""

    def __init__(self, blobs: dict[str, bytes]):
        """Build fake blobs from a ``{name: bytes}`` mapping."""
        self._blobs = {name: FakeBlob(name, data, self) for name, data in blobs.items()}
        self.list_prefixes: list[str | None] = []
        self.compose_calls: list[str] = []

    def list_blobs(self, prefix: str | None = None) -> list[FakeBlob]:
        """Return blobs (name-sorted, like GCS) whose name matches ``prefix``."""
        self.list_prefixes.append(prefix)
        return [
            blob
            for name, blob in sorted(self._blobs.items())
            if prefix is None or name.startswith(prefix)
        ]

    def blob(self, name: str) -> FakeBlob:
        """Return a blob handle; it isn't stored until it's written."""
        return FakeBlob(name, bucket=self)

    def get_blob(self, name: str) -> FakeBlob | None:
        """Return the stored blob, or ``None`` if it doesn't exist."""
        return self._blobs.get(name)


class FakeClient:
    """Stand-in for ``google.cloud.storage.Client``."""

    def __init__(self, buckets: dict[str, dict[str, bytes]]):
        """Build fake buckets from a ``{bucket_name: {blob_name: bytes}}`` mapping."""
        self._buckets = {name: FakeBucket(blobs) for name, blobs in buckets.items()}

    def bucket(self, name: str) -> FakeBucket:
        """Return the named fake bucket."""
        return self._buckets[name]


def fake_download_many(blob_file_pairs, *, raise_exception: bool = False, **_kwargs):
    """In-process stand-in for ``transfer_manager.download_many``.

    Which keyword arguments we pass (thread pool, string filenames, skip_if_exists)
    is checked separately by spying on the call.
    """
    results = []
    for blob, path in blob_file_pairs:
        try:
            blob.download_to_filename(Path(path))
            results.append(None)
        except Exception as err:
            if raise_exception:
                raise
            results.append(err)
    return results


@pytest.fixture
def patch_download_many(monkeypatch):
    """Replace the parallel downloader with a synchronous in-process copy."""
    monkeypatch.setattr(extract.transfer_manager, "download_many", fake_download_many)


@pytest.fixture
def make_client():
    """Return a factory for a ``FakeClient`` from ``{bucket: {blob: bytes}}``."""
    return FakeClient


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
def filtered_blob_names(make_client, partition_context):
    """Return a function listing the blob names an extractor keeps for a partition.

    Builds the extractor with a fake bucket holding empty blobs of the given names,
    so tests only need to give the names and the expected survivors.
    """

    def _filter(extractor_cls, partition_key, blob_names, **kwargs) -> list[str]:
        bucket_name = extractor_cls(**kwargs).bucket_name
        client = make_client({bucket_name: dict.fromkeys(blob_names, b"")})
        extractor = extractor_cls(client=client, **kwargs)
        listed = client.bucket(bucket_name).list_blobs()
        kept = extractor.filter_blobs(partition_context(partition_key), listed)
        return [blob.name for blob in kept]

    return _filter
