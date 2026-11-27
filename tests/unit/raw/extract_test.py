"""Tests for the GCSExtractor base class."""

from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from dagster import Failure, build_asset_context

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


def test_combine_files_separates_files_with_one_newline(tmp_path):
    """A missing trailing newline is added, and empty files contribute nothing."""
    files = _write_files(tmp_path, b"a 1\n", b"", b"b 2")
    dest = LineExtractor().combine_files(files, tmp_path / "out")
    assert dest.read_bytes() == b"a 1\nb 2\n"


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


def test_extract_no_files_fails_when_files_are_required(
    partition_context, run_extract, download_dir
):
    """No retries, and no empty frame to write over the partition's existing output."""

    class RequiredExtractor(LineExtractor):
        require_files = True

    with pytest.raises(Failure) as caught:
        run_extract(RequiredExtractor(), partition_context("2024-01-01"), [])
    assert caught.value.allow_retries is False


def test_only_sources_with_expiring_files_require_them():
    assert EelHoleExtractor.require_files is True
    assert GCSExtractor.require_files is False
    assert S3Extractor.require_files is False


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


@pytest.mark.parametrize(
    "partition_key",
    ["2026-02-25", None],
    ids=["partitioned", "non_partitioned"],
)
def test_extract_sets_partition_key_before_load(
    run_extract, monkeypatch, download_dir, tmp_path, partition_key
):
    """load_file can rely on self.partition_key, which is None for unpartitioned runs
    (e.g. the cumulative GitHub metrics)."""
    files = _write_files(tmp_path, b"x 1\n")
    ext = LineExtractor()
    seen = []
    monkeypatch.setattr(
        ext,
        "load_file",
        lambda p: seen.append(ext.partition_key) or pl.DataFrame({"v": [1]}),
    )
    df = run_extract(ext, build_asset_context(partition_key=partition_key), files)
    assert seen == [partition_key]
    assert not df.empty


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


def test_download_gcs_blobs_lists_by_prefix_and_flattens_names(
    partition_context, patch_download_many, fake_gcs, download_dir
):
    """Only blobs under the prefix are downloaded, with "/" in their names replaced."""
    blobs = {"logs/2024-01-01/aa": b"a 1\n", "logs/2024-01-02/bb": b"b 2\n"}
    client = fake_gcs({"test-bucket": blobs})

    class PrefixExtractor(LineExtractor):
        def get_blob_prefix(self, context):
            return "logs/2024-01-01"

    ext = PrefixExtractor(client=client)
    paths = ext.download_gcs_blobs(partition_context("2024-01-01"), download_dir)
    assert [p.name for p in paths] == ["logs-2024-01-01-aa"]
    assert paths[0].read_bytes() == b"a 1\n"
