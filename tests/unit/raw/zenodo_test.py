"""Tests for the Zenodo log extractor."""

import json

from usage_metrics.raw.zenodo import ZenodoExtractor


def _hits(*ids: int) -> bytes:
    return json.dumps({"hits": {"hits": [{"id": i} for i in ids]}}).encode()


def test_filter_blobs_keeps_only_the_days_json(filtered_blob_names):
    """Reading other days' archives would put one day's rows in several partitions."""
    kept = filtered_blob_names(
        ZenodoExtractor,
        "2024-01-01",
        [
            "zenodo/2024-01-01-111.json",
            "zenodo/2024-01-01-222.json",  # a second archive the same day
            "zenodo/2024-01-02-333.json",  # the next day
            "zenodo/2024-01-01-444.csv",  # wrong extension
            "zenodo/2024-01-01-notanid.json",  # id isn't numeric
        ],
    )
    assert kept == ["zenodo/2024-01-01-111.json", "zenodo/2024-01-01-222.json"]


def test_load_file_dates_rows_from_filename(tmp_path):
    """metrics_date comes from the file name, not the partition key."""
    path = tmp_path / "zenodo-2024-01-03-222.json"
    path.write_bytes(_hits(1, 2))
    df = ZenodoExtractor().load_file(path)
    assert list(df["id"]) == [1, 2]
    assert set(df["metrics_date"]) == {"2024-01-03"}
    assert set(df["source_record_id"]) == {222}


def test_extract_keeps_what_each_file_name_says(extractor_with, partition_context):
    """Each file's rows keep that file's record id (files are not concatenated)."""
    ext = extractor_with(
        ZenodoExtractor,
        {
            "zenodo/2024-01-01-111.json": _hits(1),
            "zenodo/2024-01-01-222.json": _hits(2),
        },
    )
    df = ext.extract(partition_context("2024-01-01"))
    assert dict(zip(df["id"], df["source_record_id"], strict=True)) == {1: 111, 2: 222}
    assert set(df["metrics_date"]) == {"2024-01-01"}
