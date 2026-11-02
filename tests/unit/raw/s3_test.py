"""Tests for the S3 log extractor."""

import polars as pl
import pytest
from dagster import materialize

from usage_metrics.raw.s3 import S3Extractor, raw_s3_logs

# --- filter_blobs ---------------------------------------------------------


def test_filter_blobs_keeps_only_matching_date(filtered_blob_names):
    """filter_blobs keeps blobs whose name starts with the partition date."""
    kept = filtered_blob_names(
        S3Extractor,
        "2024-06-15",
        ["2024-06-15-00-00-00-a", "2024-06-16-00-00-00-b", "2024-06-15"],
    )
    assert kept == ["2024-06-15", "2024-06-15-00-00-00-a"]


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


# --- raw_s3_logs asset ---------------------------------------------------


@pytest.mark.parametrize(
    ("subdir", "n_columns"),
    [("normal_v1", 27), (None, None)],
    ids=["with_logs", "empty_partition"],
)
def test_raw_s3_logs_asset_materializes(
    s3_fixture_blobs,
    make_client,
    patch_download_many,
    download_dir,
    monkeypatch,
    subdir,
    n_columns,
):
    """End to end: fake bucket -> raw_s3_logs -> one pandas frame (or an empty one)."""
    blobs = s3_fixture_blobs(subdir) if subdir else {}
    client = make_client({S3Extractor().bucket_name: blobs})
    monkeypatch.setattr(
        "usage_metrics.raw.s3.S3Extractor",
        lambda *a, **k: S3Extractor(client=client),
    )
    result = materialize([raw_s3_logs], partition_key="2024-06-15")
    assert result.success
    df = result.output_for_node("raw_s3_logs")
    if not blobs:
        assert df.empty
        return
    assert len(df) == sum(v.count(b"\n") for v in blobs.values())
    assert df.shape[1] == n_columns
