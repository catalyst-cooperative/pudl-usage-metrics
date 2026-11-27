"""Tests for the eel hole (PUDL Viewer) log extractor."""

import pytest
from dagster import Failure, build_asset_context

from usage_metrics.raw import eel_hole
from usage_metrics.raw.eel_hole import EelHoleExtractor, raw_eel_hole_logs

STDOUT = "run.googleapis.com/stdout"


def test_filter_blobs_keeps_only_matching_day(filtered_blob_names):
    """filter_blobs keeps blobs under the partition day's prefix."""
    day = f"{STDOUT}/2026/09/01"
    kept = filtered_blob_names(
        EelHoleExtractor,
        "2026-09-01",
        [
            f"{day}/00:00:00_00:59:59_S0.json",
            f"{day}/01:00:00_01:59:59_S0.json",
            f"{STDOUT}/2026/09/02/00:00:00_00:59:59_S0.json",
        ],
    )
    assert kept == [
        f"{day}/00:00:00_00:59:59_S0.json",
        f"{day}/01:00:00_01:59:59_S0.json",
    ]


def test_a_partly_expired_day_is_not_processed(
    fake_gcs, patch_download_many, download_dir, monkeypatch
):
    """What's left of its logs could replace the day's complete outputs."""
    name = f"{STDOUT}/2026/05/27/00:00:00_00:59:59_S0.json"
    bucket = EelHoleExtractor().bucket_name
    client = fake_gcs({bucket: {name: b'{"insertId": "a"}\n'}})
    monkeypatch.setattr(
        eel_hole, "EelHoleExtractor", lambda: EelHoleExtractor(client=client)
    )

    with pytest.raises(Failure):
        raw_eel_hole_logs(build_asset_context(partition_key="2026-05-27"))
