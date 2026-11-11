"""Tests for the GCSExtractor base class."""

from pathlib import Path
from typing import Any
from unittest import mock

import pandas as pd
import polars as pl
import pytest
from dagster import build_asset_context

from usage_metrics.raw import extract
from usage_metrics.raw.eel_hole import EelHoleExtractor
from usage_metrics.raw.extract import GCSExtractor
from usage_metrics.raw.github_partitioned import GithubExtractor
from usage_metrics.raw.s3 import S3Extractor


class LineExtractor(GCSExtractor):
    """Minimal concatenable extractor for exercising the base class."""

    dataset_name = "test_lines"
    bucket_name = "test-bucket"
    concatenable_files = True

    def filter_blobs(self, context, blobs):
        """Keep every blob."""
        return list(blobs)

    def load_file(self, file_path: Path) -> pl.DataFrame:
        """Parse a whitespace-delimited file."""
        return pl.read_csv(
            file_path,
            separator=" ",
            has_header=False,
            infer_schema=False,
        )


class DocExtractor(LineExtractor):
    """Like LineExtractor but one-file-at-a-time."""

    dataset_name = "test_docs"
    concatenable_files = False


# --- combine_files -----------------------------------------------------------


def test_combine_files_inserts_single_newline(tmp_path):
    """Files are joined with exactly one newline, even without a trailing one."""
    a = tmp_path / "a"
    a.write_bytes(b"a 1\n")
    b = tmp_path / "b"
    b.write_bytes(b"b 2")  # no trailing newline
    dest = LineExtractor().combine_files([a, b], tmp_path / "out")
    assert dest.read_bytes() == b"a 1\nb 2\n"


def test_combine_files_skips_empty_files(tmp_path):
    """Empty inputs contribute nothing (no blank rows)."""
    a = tmp_path / "a"
    a.write_bytes(b"a 1\n")
    empty = tmp_path / "empty"
    empty.write_bytes(b"")
    dest = LineExtractor().combine_files([a, empty], tmp_path / "out")
    assert dest.read_bytes() == b"a 1\n"


# --- extract() dispatch ----------------------------------------------------


def _write_files(directory: Path, *contents: bytes) -> list[Path]:
    """Write each of ``contents`` to its own file and return the paths."""
    paths = []
    for i, content in enumerate(contents):
        path = directory / f"file{i}"
        path.write_bytes(content)
        paths.append(path)
    return paths


@pytest.fixture
def loaded_paths(monkeypatch):
    """Spy on ``load_file`` for an extractor; the list fills with the paths parsed."""

    def _spy(extractor) -> list[Path]:
        seen: list[Path] = []
        monkeypatch.setattr(
            extractor, "load_file", lambda p: seen.append(p) or pl.DataFrame()
        )
        return seen

    return _spy


def test_extract_no_files_returns_empty(partition_context, run_extract, download_dir):
    """A partition with no blobs yields an empty DataFrame."""
    df = run_extract(LineExtractor(), partition_context("2024-01-01"), [])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.parametrize("n_files", [1, 2])
def test_extract_combines_only_multiple_files(
    partition_context, run_extract, loaded_paths, download_dir, tmp_path, n_files
):
    """One file is parsed directly; several concatenable files are combined first."""
    files = _write_files(tmp_path, *[b"x 1\n"] * n_files)
    ext = LineExtractor()
    seen = loaded_paths(ext)
    run_extract(ext, partition_context("2024-01-01"), files)
    combined = download_dir / "raw" / "test_lines" / "2024-01-01.combined"
    assert seen == ([files[0]] if n_files == 1 else [combined])
    assert combined.exists() == (n_files > 1)


def test_extract_parses_each_file_when_not_concatenable(
    partition_context, run_extract, monkeypatch, download_dir, tmp_path
):
    """Non-concatenable extractors parse per file and concatenate the frames."""
    files = _write_files(tmp_path, b"1\n", b"2\n")
    ext = DocExtractor()
    monkeypatch.setattr(
        ext, "load_file", lambda p: pd.DataFrame({"v": [p.read_text().strip()]})
    )
    df = run_extract(ext, partition_context("2024-01-01"), files)
    assert df["v"].tolist() == ["1", "2"]


def test_extract_non_partitioned_run(monkeypatch, download_dir, tmp_path):
    """extract() works for a non-partitioned run (e.g. cumulative GitHub metrics)."""
    files = _write_files(tmp_path, b"x 1\n")
    ext = DocExtractor()
    monkeypatch.setattr(ext, "download_gcs_blobs", lambda *a, **k: files)
    df = ext.extract(build_asset_context())  # no partition_key
    assert ext.partition_key is None
    assert not df.empty


def test_extract_sets_partition_key_before_load(
    partition_context, run_extract, monkeypatch, download_dir, tmp_path
):
    """load_file can rely on self.partition_key."""
    files = _write_files(tmp_path, b"x 1\n")
    ext = LineExtractor()
    captured = {}
    monkeypatch.setattr(
        ext,
        "load_file",
        lambda p: captured.setdefault("key", ext.partition_key) or pl.DataFrame(),
    )
    run_extract(ext, partition_context("2026-02-25"), files)
    assert captured["key"] == "2026-02-25"


def test_extract_swallows_empty_data_errors(
    partition_context, run_extract, monkeypatch, download_dir, tmp_path
):
    """An all-empty partition returns an empty DataFrame, not an error."""
    files = _write_files(tmp_path, b"")
    ext = LineExtractor()

    def _raise(_path):
        raise pl.exceptions.NoDataError("empty")

    monkeypatch.setattr(ext, "load_file", _raise)
    df = run_extract(ext, partition_context("2024-01-01"), files)
    assert df.empty


def test_extract_propagates_other_errors(
    partition_context, run_extract, monkeypatch, download_dir, tmp_path
):
    """Non-empty-data parse errors are not swallowed."""
    files = _write_files(tmp_path, b"x")
    ext = LineExtractor()

    def _raise(_path):
        raise ValueError("boom")

    monkeypatch.setattr(ext, "load_file", _raise)
    with pytest.raises(ValueError, match="boom"):
        run_extract(ext, partition_context("2024-01-01"), files)


@pytest.mark.parametrize(
    ("extractor_cls", "kwargs", "partition_key", "expected"),
    [
        (EelHoleExtractor, {}, "2026-09-01", "run.googleapis.com/stdout/2026/09/01"),
        (S3Extractor, {}, "2024-06-15", "2024-06-15"),
        (GithubExtractor, {"metric": "clones"}, "2024-06-15", "github/clones/"),
    ],
    ids=["eel_hole", "s3", "github"],
)
def test_get_blob_prefix(
    partition_context, extractor_cls, kwargs, partition_key, expected
):
    """Each extractor narrows the bucket listing server-side to its partition."""
    ext = extractor_cls(**kwargs)
    assert ext.get_blob_prefix(partition_context(partition_key)) == expected


# --- download plumbing ---------------------------------------------------


def test_download_gcs_blobs_uses_prefix_and_injected_client(
    partition_context, patch_download_many, make_client, download_dir, monkeypatch
):
    """The listing is prefixed and the injected client is used (no real GCS)."""
    blobs = {"2024-01-01-aa": b"a 1\n", "2024-01-02-bb": b"b 2\n"}
    client = make_client({"test-bucket": blobs})

    class PrefixExtractor(LineExtractor):
        def get_blob_prefix(self, context):
            return "2024-01-01"

    ext = PrefixExtractor(client=client)
    paths = ext.download_gcs_blobs(partition_context("2024-01-01"), download_dir)
    assert [p.name for p in paths] == ["2024-01-01-aa"]
    assert paths[0].read_bytes() == b"a 1\n"


def test_get_blobs_from_gcs_flattens_names(
    patch_download_many, make_client, download_dir
):
    """Folder separators in blob names become hyphens in local paths."""
    client = make_client({"test-bucket": {"a/b/c": b"data\n"}})
    ext = LineExtractor(client=client)
    blobs = client.bucket("test-bucket").list_blobs()
    paths = ext.get_blobs_from_gcs(blobs, download_dir)
    assert paths[0].name == "a-b-c"


def test_get_blobs_from_gcs_passes_string_filenames(
    make_client, download_dir, monkeypatch
):
    """download_many gets (blob, str) pairs on a THREAD pool, not Path objects.

    transfer_manager rejects non-str targets for its process pool and silently
    ignores skip_if_exists for them.
    """
    captured: dict[str, Any] = {}

    def spy(pairs, **kwargs):
        captured["pairs"] = list(pairs)
        captured["kwargs"] = kwargs

    monkeypatch.setattr(extract.transfer_manager, "download_many", spy)
    client = make_client({"test-bucket": {"a/b": b""}})
    ext = LineExtractor(client=client)
    ext.get_blobs_from_gcs(client.bucket("test-bucket").list_blobs(), download_dir)

    (_blob, target) = captured["pairs"][0]
    assert isinstance(target, str)
    assert captured["kwargs"]["worker_type"] == extract.transfer_manager.THREAD
    assert captured["kwargs"]["skip_if_exists"] is True
    assert captured["kwargs"]["max_workers"] == ext.download_workers


def test_download_workers_resolution(monkeypatch):
    """download_workers: explicit arg > env var > default."""
    monkeypatch.delenv("GCS_DOWNLOAD_WORKERS", raising=False)
    assert LineExtractor().download_workers == extract.DEFAULT_DOWNLOAD_WORKERS
    monkeypatch.setenv("GCS_DOWNLOAD_WORKERS", "9")
    assert LineExtractor().download_workers == 9
    assert LineExtractor(download_workers=3).download_workers == 3


def test_gcs_client_is_lazy_and_sizes_the_pool(monkeypatch):
    """No client until first use; then a sized HTTPS adapter is mounted."""
    fake = mock.Mock()
    calls = []
    monkeypatch.setattr(
        extract.storage, "Client", lambda *a, **k: calls.append(1) or fake
    )
    ext = LineExtractor(download_workers=17)
    assert calls == []
    assert ext.gcs_client is fake
    assert calls == [1]
    fake._http.mount.assert_called_once()
    assert fake._http.mount.call_args.args[0] == "https://"
    adapter = fake._http.mount.call_args.args[1]
    assert adapter._pool_maxsize == 17


def test_retry_policy_shape():
    """Guard the extract retry policy against accidental changes."""
    assert extract.GCS_EXTRACT_RETRY_POLICY.max_retries == 2
    assert extract.GCS_EXTRACT_RETRY_POLICY.delay == 30
