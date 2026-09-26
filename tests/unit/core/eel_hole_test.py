"""Tests for usage_metrics.core.eel_hole."""

import pandas as pd
import pytest
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


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"name": "x"},
        {"name": "x", "page": 1},
    ],
    ids=["empty", "missing_most_fields", "missing_filters_and_perPage"],
)
def test_drops_malformed_params_instead_of_raising(params):
    """A `params` dict missing required DuckDBParams fields is dropped, not raised.

    Regression test: `jsonPayload.params = {}` (and other incomplete shapes)
    used to sail through to `DuckDBParams` validation, which requires every
    field, raising a `ValidationError` and failing the whole partition. The
    row should just fall out (no event survives to the output) instead.
    """
    raw = pd.DataFrame([_row("duckdb_preview", params=params)])
    context = build_asset_context(partition_key="2026-09-01")
    df = _core_eel_hole_logs(context, raw)
    assert df.empty


def test_keeps_events_with_complete_params():
    """A fully-populated `params` dict still parses and survives."""
    params = {"filters": "[]", "name": "x", "page": 1, "perPage": 10}
    raw = pd.DataFrame([_row("duckdb_preview", params=params)])
    context = build_asset_context(partition_key="2026-09-01")
    df = _core_eel_hole_logs(context, raw)
    assert list(df["event"]) == ["duckdb_preview"]


def test_accepts_ends_with_filter_operation():
    """An `endsWith` search filter parses instead of raising.

    Regression test: `DuckDBFilters.operation`'s allowed values included
    `startsWith` but not its counterpart `endsWith` -- an easy oversight to
    miss, but a real filter operation the viewer's search UI offers, so a
    `ValidationError` failed the whole partition instead of just being an
    unrecognized/malformed shape.
    """
    params = {
        "filters": [
            {
                "fieldName": "utility_name",
                "fieldType": "text",
                "operation": "endsWith",
                "value": "Co",
            }
        ],
        "name": "x",
        "page": 1,
        "perPage": 10,
    }
    raw = pd.DataFrame([_row("duckdb_preview", params=params)])
    context = build_asset_context(partition_key="2026-09-17")
    df = _core_eel_hole_logs(context, raw)
    assert list(df["event"]) == ["duckdb_preview"]


def test_accepts_mismatched_case_filter_values():
    """Filters with unexpectedly-cased `operation`/`field_type` values still parse.

    Regression test: the viewer's search UI doesn't consistently match the
    casing used elsewhere (e.g. `inrange` instead of `inRange`), and has been
    observed sending `field_type: "string"` where `"text"` was expected. These
    used to raise a `ValidationError` and fail the whole partition instead of
    just being normalized.
    """
    params = {
        "filters": [
            {
                "fieldName": "utility_name",
                "fieldType": "STRING",
                "operation": "inrange",
                "value": "Co",
            }
        ],
        "name": "x",
        "page": 1,
        "perPage": 10,
    }
    raw = pd.DataFrame([_row("duckdb_preview", params=params)])
    context = build_asset_context(partition_key="2026-08-28")
    df = _core_eel_hole_logs(context, raw)
    assert list(df["event"]) == ["duckdb_preview"]
    assert df["params_filters_field_type"].iloc[0] == "text"
    assert df["params_filters_operation"].iloc[0] == "inRange"


def test_tolerates_a_partition_with_no_parseable_payloads():
    """A partition of nothing but app noise (no jsonPayload) shouldn't KeyError.

    Regression test: when no record in the partition has a jsonPayload at all,
    `pd.json_normalize` never creates any `json_payload_*` column, so selecting
    e.g. `.event` downstream raised `KeyError` instead of producing an empty
    result.
    """
    noise_row = _row("hit") | {"jsonPayload": None}
    raw = pd.DataFrame([noise_row])
    context = build_asset_context(partition_key="2026-09-01")
    df = _core_eel_hole_logs(context, raw)
    assert df.empty
