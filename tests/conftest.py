"""PyTest configuration module. Defines fixtures shared across the tests.

The fake GCS classes only store, list, and concatenate bytes. They don't enforce GCS's
limits, and the only precondition they check is ``if_generation_match`` on uploads.
Tests get them through the fixtures below.
"""

import copy
import logging
import os
from pathlib import Path

import pandera.pyarrow as pandera
import pytest
from google.api_core.exceptions import PreconditionFailed

from usage_metrics.models import _column, _table_schema
from usage_metrics.raw import extract

# Every Dagster build_*_context() call creates a throwaway instance with an in-memory
# SQLite database, and setting that up runs Alembic migrations that Alembic logs at
# INFO. pytest shows those live (log_cli_level = "INFO" in pyproject.toml), which
# drowns out the test output.
logging.getLogger("alembic").setLevel(logging.WARNING)


@pytest.fixture
def data_dir(tmp_path, mocker) -> Path:
    """Keep everything stored locally, e.g. downloads, in a temporary directory."""
    mocker.patch.dict(os.environ, {"PUDL_METRICS_LOCAL_DATA_DIR": str(tmp_path)})
    return tmp_path


@pytest.fixture
def toy_schema() -> pandera.DataFrameSchema:
    """A small table schema with a primary key and a column with a pattern."""
    return _table_schema(
        "toy",
        [
            _column("id", str, "ID."),
            _column("tls", str, "TLS version.", pattern=r"-|TLSv1\.[0-3]"),
        ],
        "A toy table.",
        primary_key=["id"],
    )


class FakeBlob:
    """Stand-in for ``google.cloud.storage.Blob`` backed by in-memory bytes."""

    def __init__(
        self,
        name: str,
        data: bytes = b"",
        bucket: FakeBucket | None = None,
        time_created=None,
    ):
        """Store the blob name and contents."""
        self.name = name
        self._data = data
        self.bucket = bucket
        self.time_created = time_created
        self.metadata: dict[str, str] | None = None
        self.generation = 1

    @property
    def size(self) -> int:
        """Size of the blob in bytes."""
        return len(self._data)

    def download_as_bytes(self) -> bytes:
        """Return the blob contents."""
        return self._data

    def download_to_filename(self, filename) -> None:
        """Write the blob contents to a local path."""
        Path(filename).write_bytes(self._data)

    def upload_from_string(self, data: bytes, if_generation_match=None) -> None:
        """Set the contents and store a snapshot of the blob in its bucket.

        As in GCS, changing ``metadata`` after an upload doesn't change what's stored.
        """
        assert self.bucket is not None
        previous = self.bucket.get_blob(self.name)
        generation = previous.generation if previous else 0
        if if_generation_match not in (None, generation):
            raise PreconditionFailed(f"{self.name} is at generation {generation}.")
        self._data = data
        stored = copy.copy(self)
        stored.generation = generation + 1
        stored.metadata = copy.copy(self.metadata)
        self.bucket._blobs[self.name] = stored

    def upload_from_filename(
        self, filename, content_type=None, if_generation_match=None
    ) -> None:
        """Upload the contents of a local file."""
        self.upload_from_string(Path(filename).read_bytes(), if_generation_match)

    def compose(self, sources, if_generation_match=None) -> None:
        """Store the concatenation of ``sources`` in this blob."""
        assert self.bucket is not None
        self.bucket.compose_calls.append(self.name)
        self.upload_from_string(b"".join(s.download_as_bytes() for s in sources))


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

    Like the real function, it returns a result for each blob, holding any exception
    instead of raising it unless ``raise_exception`` is set. Other keyword arguments
    are ignored.
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
def fake_gcs():
    """Return a factory for a ``FakeClient`` from ``{bucket: {blob: bytes}}``."""
    return FakeClient


@pytest.fixture
def fake_blob():
    """Return the ``FakeBlob`` class, to make blobs that aren't in a fake bucket."""
    return FakeBlob
