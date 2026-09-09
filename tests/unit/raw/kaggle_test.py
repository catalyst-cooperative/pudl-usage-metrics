"""Tests for the Kaggle log extractor."""

import json

import pytest
from dagster import build_asset_context

from usage_metrics.raw.kaggle import KaggleExtractor

BUCKET = "pudl-usage-metrics-archives.catalyst.coop"


def _ctx(partition_key: str):
    return build_asset_context(partition_key=partition_key)


def test_filter_blobs_matches_exact_daily_file(make_client):
    """filter_blobs keeps only the single ``kaggle/<date>.json`` object."""
    blobs = {
        "kaggle/2025-10-01.json": b"{}",
        "kaggle/2025-10-02.json": b"{}",
        "kaggle/2025-10-01.json.bak": b"{}",
    }
    client = make_client({BUCKET: blobs})
    ext = KaggleExtractor(client=client)
    listed = client.bucket(BUCKET).list_blobs()
    kept = [blob.name for blob in ext.filter_blobs(_ctx("2025-10-01"), listed)]
    assert kept == ["kaggle/2025-10-01.json"]


def test_load_file_new_format(tmp_path):
    """From 2025-09-21 on, fields are top-level and normalized directly."""
    path = tmp_path / "kaggle-2025-10-01.json"
    path.write_text(json.dumps({"totalViews": 5, "totalDownloads": 2}))
    ext = KaggleExtractor()
    ext.partition_key = "2025-10-01"
    df = ext.load_file(path)
    assert df.loc[0, "totalViews"] == 5


def test_load_file_old_format_adds_metrics_date(tmp_path):
    """Before 2025-09-21, data is nested under ``info`` with a sibling date."""
    path = tmp_path / "kaggle-2024-01-01.json"
    path.write_text(
        json.dumps({"info": {"totalViews": 3}, "metrics_date": "2024-01-01"})
    )
    ext = KaggleExtractor()
    ext.partition_key = "2024-01-01"
    df = ext.load_file(path)
    assert df.loc[0, "totalViews"] == 3
    assert df.loc[0, "metrics_date"] == "2024-01-01"


def test_load_file_raises_on_api_error_payload(tmp_path):
    """A Kaggle error response is surfaced as an assertion error."""
    path = tmp_path / "kaggle-2025-10-01.json"
    path.write_text(json.dumps({"hasErrorMessage": True, "errorMessage": "nope"}))
    ext = KaggleExtractor()
    ext.partition_key = "2025-10-01"
    with pytest.raises(AssertionError, match="nope"):
        ext.load_file(path)
