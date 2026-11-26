"""Tests for the eel hole (PUDL Viewer) log extractor."""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import cast

import pytest
from dagster import build_asset_context

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


@pytest.mark.parametrize(
    ("day", "now", "produced"),
    [
        pytest.param(
            "2026-05-27", datetime(2026, 6, 1, tzinfo=UTC), False, id="partly_expired"
        ),
        pytest.param(
            "2026-05-28", datetime(2026, 6, 1, tzinfo=UTC), True, id="first_complete"
        ),
        pytest.param(
            "2026-09-01", datetime(2026, 9, 2, 1, tzinfo=UTC), False, id="not_yet_due"
        ),
    ],
)
def test_only_complete_days_are_processed(
    fake_gcs, patch_download_many, download_dir, monkeypatch, day, now, produced
):
    """Raw files for only part of a day are left alone: processing them could replace
    the day's complete outputs.
    """
    name = f"{STDOUT}/{day.replace('-', '/')}/00:00:00_00:59:59_S0.json"
    bucket = EelHoleExtractor().bucket_name
    client = fake_gcs({bucket: {name: b'{"insertId": "a"}\n'}})
    monkeypatch.setattr(
        eel_hole, "EelHoleExtractor", lambda: EelHoleExtractor(client=client)
    )
    monkeypatch.setattr(eel_hole, "_utcnow", lambda: now)

    outputs = raw_eel_hole_logs(build_asset_context(partition_key=day))

    assert bool(list(cast(Iterator, outputs))) is produced
