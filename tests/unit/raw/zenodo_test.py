"""Tests for the Zenodo log extractor."""

import json

from usage_metrics.raw.zenodo import ZenodoExtractor


def _hits(*ids: int) -> bytes:
    return json.dumps({"hits": {"hits": [{"id": i} for i in ids]}}).encode()


def test_filter_blobs_keeps_in_range_json_only(filtered_blob_names):
    """Only ``zenodo/<date>-<id>.json`` files within the 7-day window are kept."""
    kept = filtered_blob_names(
        ZenodoExtractor,
        "2024-01-01",
        [
            "zenodo/2024-01-01-111.json",  # in window
            "zenodo/2024-01-07-222.json",  # in window (last day)
            "zenodo/2024-01-09-333.json",  # out of window
            "zenodo/2024-01-03-444.csv",  # wrong extension
            "zenodo/2024-01-05-notanid.json",  # id isn't numeric
        ],
    )
    assert kept == ["zenodo/2024-01-01-111.json", "zenodo/2024-01-07-222.json"]


def test_load_file_dates_rows_from_filename(tmp_path):
    """metrics_date comes from the file name, not the partition key."""
    path = tmp_path / "zenodo-2024-01-03-222.json"
    path.write_bytes(_hits(1, 2))
    df = ZenodoExtractor().load_file(path)
    assert list(df["id"]) == [1, 2]
    assert set(df["metrics_date"]) == {"2024-01-03"}
    assert set(df["source_record_id"]) == {222}


def test_extract_keeps_per_file_metrics_date(
    download_dir, run_extract, partition_context
):
    """Each file's rows keep that file's date (files are not concatenated)."""
    assert ZenodoExtractor.concatenable_files is False
    a = download_dir / "zenodo-2024-01-01-111.json"
    a.write_bytes(_hits(111))
    b = download_dir / "zenodo-2024-01-03-222.json"
    b.write_bytes(_hits(222))
    df = run_extract(ZenodoExtractor(), partition_context("2024-01-01"), [a, b])
    assert dict(zip(df["id"], df["metrics_date"], strict=True)) == {
        111: "2024-01-01",
        222: "2024-01-03",
    }
