"""Tests for the S3 log extractor."""

import polars as pl
import pytest
from dagster import build_asset_context, materialize

from usage_metrics.raw.s3 import S3Extractor, raw_s3_logs

BUCKET = "pudl-s3-logs.catalyst.coop"


def _ctx(partition_key: str):
    return build_asset_context(partition_key=partition_key)


# --- get_blob_prefix / filter_blobs ----------------------------------------


def test_get_blob_prefix_is_partition_date():
    """The listing prefix is the partition's ISO date."""
    ext = S3Extractor()
    assert ext.get_blob_prefix(_ctx("2024-06-15")) == "2024-06-15"


def test_filter_blobs_keeps_only_matching_date(make_client):
    """filter_blobs keeps blobs whose name starts with the partition date."""
    blobs = {
        "2024-06-15-00-00-00-a": b"",
        "2024-06-16-00-00-00-b": b"",
        "2024-06-15": b"",
    }
    client = make_client({BUCKET: blobs})
    ext = S3Extractor(client=client)
    listed = client.bucket(BUCKET).list_blobs()
    kept = {b.name for b in ext.filter_blobs(_ctx("2024-06-15"), listed)}
    assert kept == {"2024-06-15-00-00-00-a", "2024-06-15"}


# --- load_file -----------------------------------------------------------


@pytest.mark.parametrize(
    ("subdir", "n_columns"),
    [("normal_v1", 27), ("normal_v2", 28)],
)
def test_load_file_column_counts(tmp_path, s3_fixture_blobs, subdir, n_columns):
    """Each schema era parses to the expected column count."""
    ext = S3Extractor()
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs(subdir).values()))
    df = ext.load_file(combined)
    assert df.width == n_columns


def test_load_file_preserves_quoted_and_dash_fields(tmp_path):
    """Quoted fields with spaces stay intact; '-' is read literally."""
    line = (
        "owner bkt [15/Jun/2024:00:00:00 +0000] 198.51.100.1 - RID REST.GET.OBJECT k "
        '"GET /k HTTP/1.1" 200 - 100 200 5 4 "-" "Mozilla/5.0 (X11; Linux)" - hid '
        "SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )
    path = tmp_path / "f"
    path.write_text(line)
    row = S3Extractor().load_file(path).row(0)
    assert row[9] == "GET /k HTTP/1.1"
    assert row[17] == "Mozilla/5.0 (X11; Linux)"
    assert row[5] == "-"


def test_load_file_ragged_partition_forces_28_columns(tmp_path, s3_fixture_blobs):
    """On 2026-02-25 a mixed-width file is coerced to 28 columns."""
    ext = S3Extractor()
    ext.partition_key = "2026-02-25"
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs("ragged").values()))
    df = ext.load_file(combined)
    assert df.width == 28


def test_load_file_ragged_other_partition_raises_with_note(tmp_path, s3_fixture_blobs):
    """A ragged file outside 2026-02-25 raises, annotated with the path."""
    ext = S3Extractor()
    ext.partition_key = "2025-01-01"
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs("ragged").values()))
    with pytest.raises(pl.exceptions.ComputeError) as excinfo:
        ext.load_file(combined)
    assert any("Extraction failed" in note for note in excinfo.value.__notes__)


def test_load_file_empty_raises_no_data_error(tmp_path):
    """An empty file raises NoDataError for extract() to handle."""
    path = tmp_path / "empty"
    path.write_bytes(b"")
    with pytest.raises(pl.exceptions.NoDataError):
        S3Extractor().load_file(path)


# --- raw_s3_logs asset ---------------------------------------------------


def test_raw_s3_logs_asset_materializes(
    s3_fixture_blobs, make_client, patch_download_many, download_dir, monkeypatch
):
    """End to end: fake bucket -> raw_s3_logs -> one pandas frame."""
    blobs = s3_fixture_blobs("normal_v1")
    client = make_client({BUCKET: blobs})
    monkeypatch.setattr(
        "usage_metrics.raw.s3.S3Extractor",
        lambda *a, **k: S3Extractor(client=client),
    )
    result = materialize([raw_s3_logs], partition_key="2024-06-15")
    assert result.success
    df = result.output_for_node("raw_s3_logs")
    expected_rows = sum(v.count(b"\n") for v in blobs.values())
    assert len(df) == expected_rows
    assert df.shape[1] == 27


def test_raw_s3_logs_asset_empty_partition(
    make_client, patch_download_many, download_dir, monkeypatch
):
    """A partition with no blobs materializes an empty frame."""
    client = make_client({BUCKET: {}})
    monkeypatch.setattr(
        "usage_metrics.raw.s3.S3Extractor",
        lambda *a, **k: S3Extractor(client=client),
    )
    result = materialize([raw_s3_logs], partition_key="2024-06-15")
    assert result.success
    assert result.output_for_node("raw_s3_logs").empty
