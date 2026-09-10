"""Tests for the eel hole (PUDL Viewer) log transform assets."""

import pandas as pd
from dagster import build_asset_context

from usage_metrics.core.eel_hole import _core_eel_hole_logs

PARTITION = "2026-09-06"


def _ctx(partition_key: str = PARTITION):
    return build_asset_context(partition_key=partition_key)


def _record(
    insert_id: str,
    *,
    event: str = "search",
    params: dict | None = None,
    user_id: str | None = None,
    timestamp: str = "2026-09-06T00:00:00Z",
) -> dict:
    """Build a raw eel-hole log record shaped like the extractor output."""
    payload: dict = {"event": event, "timestamp": timestamp}
    if user_id is not None:
        payload["userId"] = user_id
    if event == "search":
        payload["url"] = "/search?q=coal"
    if params is not None:
        payload["params"] = params
    return {
        "insertId": insert_id,
        "jsonPayload": payload,
        "labels": {"instanceId": "0e1b2c3d"},
        "logName": "projects/x/logs/run.googleapis.com%2Fstdout",
        "receiveTimestamp": timestamp,
        "resource": {
            "type": "cloud_run_revision",
            "labels": {
                "configuration_name": "pudl-viewer",
                "location": "us-east1",
                "project_id": "catalyst-cooperative-pudl",
                "revision_name": "pudl-viewer-00001-abc",
                "service_name": "pudl-viewer",
            },
        },
        "timestamp": timestamp,
        "textPayload": None,
    }


_FILTER = {
    "fieldName": "report_year",
    "fieldType": "number",
    "operation": "equals",
    "value": "2022",
}
_PARAMS_WITH_FILTERS = {
    "filters": [_FILTER],
    "name": "out_eia__yearly_generators",
    "page": 1,
    "perPage": 50,
}


def test_returns_empty_frame_for_empty_input():
    """An empty raw partition yields an empty DataFrame rather than raising."""
    out = _core_eel_hole_logs(_ctx(), pd.DataFrame())
    assert out.empty


def test_handles_partition_with_no_search_filters():
    """A partition where no search event carries ``params.filters`` transforms fine.

    Regression test: ``pd.json_normalize`` only emits the
    ``json_payload_params_filters`` column when at least one record has search
    filters, so the transform must not assume that column always exists.
    """
    raw = pd.DataFrame(
        [
            _record("a", event="search", user_id="user-1"),
            _record("b", event="hit", user_id="user-1"),
        ]
    )

    out = _core_eel_hole_logs(_ctx(), raw)

    assert sorted(out["insert_id"]) == ["a", "b"]
    assert not any("filters" in col for col in out.columns)


def test_explodes_search_filters_when_present():
    """When a search carries ``params.filters`` they are split into columns."""
    raw = pd.DataFrame(
        [
            _record("a", event="search", params=_PARAMS_WITH_FILTERS, user_id="user-1"),
            _record("b", event="search", user_id="user-1"),
        ]
    )

    out = _core_eel_hole_logs(_ctx(), raw)

    assert sorted(out["insert_id"]) == ["a", "b"]
    filter_cols = [col for col in out.columns if "params_filters_" in col]
    assert filter_cols
    assert "json_payload_params_filters" not in out.columns
