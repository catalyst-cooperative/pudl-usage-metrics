"""Tests for the GCSExtractor base class."""

from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from dagster import (
    DailyPartitionsDefinition,
    Definitions,
    asset,
    build_asset_context,
    define_asset_job,
)

from usage_metrics.raw.eel_hole import EelHoleExtractor
from usage_metrics.raw.extract import GCSExtractor
from usage_metrics.raw.github_partitioned import GithubExtractor
from usage_metrics.raw.s3 import S3Extractor
from usage_metrics.scripts.etl import _execute


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


def test_extract_no_files_returns_empty(extractor_with, partition_context):
    """A partition with no blobs yields an empty DataFrame."""
    ext = extractor_with(LineExtractor, {})
    df = ext.extract(partition_context("2024-01-01"))
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_no_input_succeeds_with_a_warning_and_runs_nothing_downstream(
    fake_gcs, data_dir
):
    """A day without logs mustn't fail forever, pass unnoticed, or overwrite data."""
    client = fake_gcs({LineExtractor.bucket_name: {}})
    days = DailyPartitionsDefinition(start_date="2024-01-01")
    ran_downstream = []

    @asset(partitions_def=days, output_required=False)
    def raw(context):
        yield from LineExtractor(client=client).extract_partition(context)

    @asset(partitions_def=days)
    def core(raw):
        ran_downstream.append(raw)

    job = Definitions(
        assets=[raw, core], jobs=[define_asset_job("job")]
    ).resolve_job_def("job")

    succeeded, warnings = _execute(job, partition_key="2024-01-01")

    assert succeeded
    assert len(warnings) == 1
    assert ran_downstream == []


@pytest.mark.parametrize("n_files", [1, 2])
def test_extract_combines_only_multiple_files(
    extractor_with, partition_context, mocker, data_dir, n_files
):
    """One file is parsed directly; several concatenable files are combined first."""
    names = ["a", "b"][:n_files]
    ext = extractor_with(LineExtractor, dict.fromkeys(names, b"x 1\n"))
    load_file = mocker.spy(ext, "load_file")
    df = ext.extract(partition_context("2024-01-01"))
    downloads = data_dir / "raw" / "test_lines"
    combined = downloads / "2024-01-01.combined"
    parsed = downloads / "a" if n_files == 1 else combined
    load_file.assert_called_once_with(parsed)
    assert combined.exists() == (n_files > 1)
    assert len(df) == n_files


def test_extract_parses_each_file_when_not_concatenable(
    extractor_with, partition_context, mocker
):
    """Non-concatenable extractors parse per file and concatenate the frames."""
    ext = extractor_with(DocExtractor, {"a": b"1\n", "b": b"2\n"})
    mocker.patch.object(
        ext,
        "load_file",
        side_effect=lambda p: pd.DataFrame({"v": [p.read_text().strip()]}),
    )
    df = ext.extract(partition_context("2024-01-01"))
    assert df["v"].tolist() == ["1", "2"]


@pytest.mark.parametrize(
    "partition_key",
    ["2026-02-25", None],
    ids=["partitioned", "non_partitioned"],
)
def test_extract_sets_partition_key_before_load(extractor_with, mocker, partition_key):
    """load_file can rely on self.partition_key, which is None for unpartitioned runs
    (e.g. the cumulative GitHub metrics)."""
    ext = extractor_with(LineExtractor, {"a": b"x 1\n"})
    seen = []
    mocker.patch.object(
        ext,
        "load_file",
        side_effect=lambda p: (
            seen.append(ext.partition_key) or pl.DataFrame({"v": [1]})
        ),
    )
    df = ext.extract(build_asset_context(partition_key=partition_key))
    assert seen == [partition_key]
    assert not df.empty


def test_extract_swallows_empty_data_errors(extractor_with, partition_context):
    """An all-empty partition returns an empty DataFrame, not an error."""
    ext = extractor_with(LineExtractor, {"a": b""})
    df = ext.extract(partition_context("2024-01-01"))
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
    partition_context, patch_download_many, fake_gcs, data_dir
):
    """Only blobs under the prefix are downloaded, with "/" in their names replaced."""
    blobs = {"logs/2024-01-01/aa": b"a 1\n", "logs/2024-01-02/bb": b"b 2\n"}
    client = fake_gcs({"test-bucket": blobs})

    class PrefixExtractor(LineExtractor):
        def get_blob_prefix(self, context):
            return "logs/2024-01-01"

    ext = PrefixExtractor(client=client)
    paths = ext.download_gcs_blobs(partition_context("2024-01-01"), data_dir)
    assert [p.name for p in paths] == ["logs-2024-01-01-aa"]
    assert paths[0].read_bytes() == b"a 1\n"
