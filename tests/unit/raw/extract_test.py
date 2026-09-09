"""Tests for the GCSExtractor base class."""

from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from usage_metrics.raw import extract
from usage_metrics.raw.extract import GCSExtractor


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
            file_path, separator=" ", has_header=False, infer_schema_length=0
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


def _run_extract(extractor, context, paths, monkeypatch):
    monkeypatch.setattr(extractor, "download_gcs_blobs", lambda *a, **k: paths)
    return extractor.extract(context)


def test_extract_no_files_returns_empty(partition_context, monkeypatch, download_dir):
    """A partition with no blobs yields an empty DataFrame."""
    ext = LineExtractor()
    df = _run_extract(ext, partition_context("2024-01-01"), [], monkeypatch)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_extract_single_file_is_not_combined(
    partition_context, monkeypatch, download_dir, tmp_path
):
    """One file is parsed directly; no .combined file is written."""
    only = tmp_path / "only"
    only.write_bytes(b"x 1\ny 2\n")
    ext = LineExtractor()
    seen = []
    monkeypatch.setattr(ext, "load_file", lambda p: seen.append(p) or pl.DataFrame())
    _run_extract(ext, partition_context("2024-01-01"), [only], monkeypatch)
    assert seen == [only]
    assert not (download_dir / "test_lines" / "2024-01-01.combined").exists()


def test_extract_combines_when_concatenable(
    partition_context, monkeypatch, download_dir, tmp_path
):
    """Multiple concatenable files are combined and parsed once."""
    f1 = tmp_path / "f1"
    f1.write_bytes(b"x 1\n")
    f2 = tmp_path / "f2"
    f2.write_bytes(b"y 2\n")
    ext = LineExtractor()
    seen = []
    monkeypatch.setattr(ext, "load_file", lambda p: seen.append(p) or pl.DataFrame())
    _run_extract(ext, partition_context("2024-01-01"), [f1, f2], monkeypatch)
    assert seen == [download_dir / "test_lines" / "2024-01-01.combined"]


def test_extract_parses_each_file_when_not_concatenable(
    partition_context, monkeypatch, download_dir, tmp_path
):
    """Non-concatenable extractors parse per file and concatenate the frames."""
    f1 = tmp_path / "f1"
    f1.write_bytes(b"1\n")
    f2 = tmp_path / "f2"
    f2.write_bytes(b"2\n")
    ext = DocExtractor()
    monkeypatch.setattr(
        ext, "load_file", lambda p: pd.DataFrame({"v": [p.read_text().strip()]})
    )
    df = _run_extract(ext, partition_context("2024-01-01"), [f1, f2], monkeypatch)
    assert df["v"].tolist() == ["1", "2"]


def test_extract_sets_partition_key_before_load(
    partition_context, monkeypatch, download_dir, tmp_path
):
    """load_file can rely on self.partition_key."""
    only = tmp_path / "only"
    only.write_bytes(b"x 1\n")
    ext = LineExtractor()
    captured = {}
    monkeypatch.setattr(
        ext,
        "load_file",
        lambda p: captured.setdefault("key", ext.partition_key) or pl.DataFrame(),
    )
    _run_extract(ext, partition_context("2026-02-25"), [only], monkeypatch)
    assert captured["key"] == "2026-02-25"


def test_extract_swallows_empty_data_errors(
    partition_context, monkeypatch, download_dir, tmp_path
):
    """An all-empty partition returns an empty DataFrame, not an error."""
    only = tmp_path / "only"
    only.write_bytes(b"")
    ext = LineExtractor()

    def _raise(_path):
        raise pl.exceptions.NoDataError("empty")

    monkeypatch.setattr(ext, "load_file", _raise)
    df = _run_extract(ext, partition_context("2024-01-01"), [only], monkeypatch)
    assert df.empty


def test_extract_propagates_other_errors(
    partition_context, monkeypatch, download_dir, tmp_path
):
    """Non-empty-data parse errors are not swallowed."""
    only = tmp_path / "only"
    only.write_bytes(b"x")
    ext = LineExtractor()

    def _raise(_path):
        raise ValueError("boom")

    monkeypatch.setattr(ext, "load_file", _raise)
    with pytest.raises(ValueError, match="boom"):
        _run_extract(ext, partition_context("2024-01-01"), [only], monkeypatch)


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


def test_get_blobs_from_gcs_skips_existing(
    patch_download_many, make_client, download_dir
):
    """A blob whose local file already exists is not re-downloaded."""
    client = make_client({"test-bucket": {"blob": b"new\n"}})
    ext = LineExtractor(client=client)
    (download_dir / "blob").write_bytes(b"old\n")
    blobs = client.bucket("test-bucket").list_blobs()
    paths = ext.get_blobs_from_gcs(blobs, download_dir)
    assert paths[0].read_bytes() == b"old\n"


def test_gcs_client_is_lazy(monkeypatch):
    """No client is constructed until first use."""
    calls = []
    monkeypatch.setattr(
        extract.storage, "Client", lambda *a, **k: calls.append(1) or object()
    )
    ext = LineExtractor()
    assert calls == []
    ext.gcs_client  # noqa: B018 - triggers lazy construction
    assert calls == [1]


def test_retry_policy_shape():
    """Guard the extract retry policy against accidental changes."""
    assert extract.GCS_EXTRACT_RETRY_POLICY.max_retries == 2
    assert extract.GCS_EXTRACT_RETRY_POLICY.delay == 30
