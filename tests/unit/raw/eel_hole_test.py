"""Tests for the eel hole (PUDL Viewer) log extractor."""

from usage_metrics.raw.eel_hole import EelHoleExtractor

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
