"""Tests for usage_metrics.core.eel_hole."""

import pandas as pd
from dagster import build_asset_context

from usage_metrics.core.eel_hole import _core_eel_hole_logs

RESOURCE = {
    "type": "cloud_run_revision",
    "labels": {
        "configuration_name": "eel-hole",
        "location": "us-central1",
        "project_id": "p",
        "revision_name": "eel-hole-00001",
        "service_name": "eel-hole",
    },
}


def _row(event: str, **json_payload_extra) -> dict:
    return {
        "insertId": "abc123",
        "jsonPayload": {
            "event": event,
            "timestamp": "2026-09-01T00:00:00Z",
            **json_payload_extra,
        },
        "labels": {"instanceId": "i-1"},
        "logName": "projects/x/logs/run.googleapis.com%2Fstdout",
        "receiveTimestamp": "2026-09-01T00:00:01Z",
        "resource": RESOURCE,
        "timestamp": "2026-09-01T00:00:00Z",
        "textPayload": "some log line",
    }


def test_tolerates_a_partition_with_no_search_filters():
    """A partition with eel-hole traffic but no filtered searches shouldn't crash.

    Regression test: `json_payload_params_filters` is only produced by
    `pd.json_normalize` when at least one record in the partition carries
    `jsonPayload.params.filters`. A partition where every event lacks
    `params` (e.g. a day with only `hit` events, no `duckdb_preview`/
    `duckdb_csv` searches) never creates that column, so unconditionally
    referencing it raised `AttributeError` and failed the whole partition.
    """
    raw = pd.DataFrame([_row("hit")])
    context = build_asset_context(partition_key="2026-09-01")
    df = _core_eel_hole_logs(context, raw)
    assert list(df["event"]) == ["hit"]
