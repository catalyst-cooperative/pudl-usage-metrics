"""Tests for the eel hole (PUDL Viewer) log transform assets."""

from collections import Counter

import pandas as pd
import pytest
from dagster import AssetCheckResult, Output, build_asset_context

from usage_metrics.core.eel_hole import (
    EEL_HOLE_SCHEMA_DRIFT_CHECK,
    EelHoleLogs,
    _core_eel_hole_logs,
    _drift_report,
    _schema_drift_check,
    payload_is_parseable,
)

PARTITION = "2026-09-06"
TS = "2026-09-06T00:00:00Z"

_UNSET = object()


def _ctx(partition_key: str = PARTITION):
    return build_asset_context(partition_key=partition_key)


def _run(raw: pd.DataFrame, ctx=None) -> pd.DataFrame:
    """Materialize ``_core_eel_hole_logs`` and return its output DataFrame."""
    results = list(_core_eel_hole_logs(ctx or _ctx(), raw))
    return next(r.value for r in results if isinstance(r, Output))


def _check(raw: pd.DataFrame, ctx=None) -> AssetCheckResult:
    """Materialize ``_core_eel_hole_logs`` and return its schema-drift check."""
    results = list(_core_eel_hole_logs(ctx or _ctx(), raw))
    return next(r for r in results if isinstance(r, AssetCheckResult))


def _drift_check(rows: list[dict], partition_key: str = PARTITION) -> AssetCheckResult:
    """Run ``_schema_drift_check`` on ``rows`` (parsing them the way the asset does)."""
    models = [EelHoleLogs(**row).model_dump() for row in rows]
    return _schema_drift_check(_ctx(partition_key), rows, models)


def _record(
    insert_id: str,
    *,
    event: str = "search",
    params: dict | None = None,
    user_id: str | None = None,
    text_payload: str | None = None,
    payload=_UNSET,
    timestamp: str = TS,
) -> dict:
    """Build a raw eel-hole log record shaped like the extractor output.

    Pass ``payload`` to set ``jsonPayload`` verbatim (including to a non-dict or
    ``None``); otherwise it is built from ``event`` / ``params`` / ``user_id``.
    """
    if payload is _UNSET:
        payload = {"event": event, "timestamp": timestamp}
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
        "textPayload": text_payload,
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
_VALID_PARAMS = {"name": "t", "page": 0, "perPage": 25, "filters": None}


# --- payload_is_parseable: the source of truth for "is this a usable event?" ---

_PARSEABLE = [
    pytest.param({"event": "search", "timestamp": TS}, id="minimal-search"),
    pytest.param({"event": "hit", "timestamp": TS, "score": None}, id="hit-null-score"),
    pytest.param({"event": "hit", "timestamp": TS, "score": 0.87}, id="hit-score"),
    pytest.param(
        {"event": "privacy-policy", "timestamp": TS, "accepted": True},
        id="privacy-policy",
    ),
    pytest.param(
        {"event": "duckdb_preview", "timestamp": TS, "params": _VALID_PARAMS},
        id="preview-valid-params",
    ),
    pytest.param(
        {"event": "search", "timestamp": TS, "params": _PARAMS_WITH_FILTERS},
        id="search-with-filters",
    ),
    pytest.param(
        {
            "event": "search",
            "timestamp": TS,
            "params": {
                "name": "t",
                "page": 1,
                "perPage": 50,
                "filters": '[{"fieldName": "y", "fieldType": "text", '
                '"operation": "contains", "value": "x"}]',
            },
        },
        id="filters-as-json-string",
    ),
    pytest.param(
        {"event": "search", "timestamp": TS, "somethingBrandNew": 123},
        id="unknown-extra-key-ignored",
    ),
]

_NOT_PARSEABLE = [
    pytest.param({}, id="empty-dict"),
    pytest.param(None, id="none"),
    pytest.param("loading data...", id="string"),
    pytest.param([1, 2, 3], id="list"),
    pytest.param(5, id="int"),
    pytest.param({"timestamp": TS}, id="missing-event"),
    pytest.param({"event": "search"}, id="missing-timestamp"),
    pytest.param({"event": "loading", "timestamp": TS}, id="unknown-event"),
    pytest.param({"event": "search", "timestamp": "not-a-date"}, id="bad-timestamp"),
    pytest.param({"event": "search", "timestamp": TS, "params": {}}, id="params-empty"),
    pytest.param(
        {"event": "search", "timestamp": TS, "params": {"name": "t"}},
        id="params-partial",
    ),
    pytest.param(
        {
            "event": "search",
            "timestamp": TS,
            "params": {"name": "t", "page": None, "perPage": 50, "filters": None},
        },
        id="params-null-page",
    ),
    pytest.param(
        {
            "event": "search",
            "timestamp": TS,
            "params": {"name": "t", "page": 1, "perPage": 50, "filters": "nonsense"},
        },
        id="filters-not-json",
    ),
    pytest.param(
        {
            "event": "search",
            "timestamp": TS,
            "params": {"name": "t", "page": 1, "perPage": 50, "filters": "{}"},
        },
        id="filters-json-but-not-a-list",
    ),
    pytest.param(
        {
            "event": "search",
            "timestamp": TS,
            "params": {
                "name": "t",
                "page": 1,
                "perPage": 50,
                "filters": [{"fieldName": "y"}],
            },
        },
        id="filter-missing-fields",
    ),
    pytest.param(
        {
            "event": "search",
            "timestamp": TS,
            "params": {
                "name": "t",
                "page": 1,
                "perPage": 50,
                "filters": [
                    {
                        "fieldName": "y",
                        "fieldType": "text",
                        "operation": "fuzzyMatch",
                        "value": "z",
                    }
                ],
            },
        },
        id="unknown-filter-operation",
    ),
]


@pytest.mark.parametrize("payload", _PARSEABLE)
def test_payload_is_parseable_true(payload):
    assert payload_is_parseable(payload) is True


@pytest.mark.parametrize("payload", _NOT_PARSEABLE)
def test_payload_is_parseable_false(payload):
    assert payload_is_parseable(payload) is False


# --- _core_eel_hole_logs -----------------------------------------------------


def test_returns_empty_frame_for_empty_input():
    """An empty raw partition yields an empty DataFrame rather than raising."""
    out = _run(pd.DataFrame())
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

    out = _run(raw)

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

    out = _run(raw)

    assert sorted(out["insert_id"]) == ["a", "b"]
    filter_cols = [col for col in out.columns if "params_filters_" in col]
    assert filter_cols
    assert "json_payload_params_filters" not in out.columns


@pytest.mark.parametrize(
    "bad_payload",
    [
        pytest.param({}, id="empty-dict"),
        pytest.param({"event": "search"}, id="missing-timestamp"),
        pytest.param({"timestamp": TS}, id="missing-event"),
        pytest.param({"event": "loading", "timestamp": TS}, id="unknown-event"),
        pytest.param("some app log line", id="string-payload"),
        pytest.param(
            {"event": "duckdb_preview", "timestamp": TS, "params": {}},
            id="empty-params",
        ),
    ],
)
def test_drops_records_with_unparseable_payload(bad_payload):
    """An unparseable payload drops that row; the partition still processes."""
    raw = pd.DataFrame(
        [
            _record("good", event="search", user_id="user-1"),
            _record("bad", payload=bad_payload, text_payload=None),
        ]
    )

    out = _run(raw)

    assert list(out["insert_id"]) == ["good"]


def test_partition_with_no_parseable_payloads_returns_empty():
    """A day of nothing but app-noise log lines yields an empty DataFrame."""
    raw = pd.DataFrame(
        [
            _record("a", payload="Starting server on :8080", text_payload="noise"),
            _record("b", payload={}, text_payload="more noise"),
            _record("c", payload=None, text_payload=None),
        ]
    )

    out = _run(raw)

    assert out.empty


def test_synthesizes_log_in_from_callback_text_payload():
    """A callback hit (text payload only, no JSON payload) becomes a log_in row."""
    raw = pd.DataFrame(
        [
            _record(
                "login",
                payload=None,
                text_payload="https://viewer.catalyst.coop/callback?next=/search?q%3Dcoal",
            ),
        ]
    )

    out = _run(raw)

    assert list(out["event"]) == ["log_in"]
    assert out.loc[0, "log_in_query"] == "coal"


# --- _schema_drift_check ---------------------------------------------------


def test_schema_drift_check_passes_for_clean_events():
    rows = [_record(str(i), event="search", user_id="u") for i in range(5)]
    result = _drift_check(rows)
    assert result.passed is True
    assert result.metadata["event_bearing_payloads"].value == 5
    assert result.metadata["dropped"].value == 0


def test_schema_drift_check_passes_with_ignored_noise_event():
    """A 'loading' noise event is dropped but doesn't count toward drift."""
    rows = [
        *(_record(f"good{i}", event="search", user_id="u") for i in range(5)),
        _record("noise", payload={"event": "loading", "timestamp": TS}),
    ]
    result = _drift_check(rows)
    assert result.passed is True
    assert result.metadata["unrecognized_event_types"].value == "none"


def test_schema_drift_check_ignores_non_slug_event_values():
    """A log message in `event` is nulled but never fails the check."""
    rows = [
        *(_record(f"g{i}", event="search", user_id="u") for i in range(5)),
        *(
            _record(
                f"log{i}",
                payload={"event": "Loading prebuilt search index", "timestamp": TS},
            )
            for i in range(6)
        ),
    ]
    result = _drift_check(rows)
    assert result.passed is True
    assert result.metadata["non_slug_event_payloads_nulled"].value == 6


def test_schema_drift_check_tolerates_a_few_bad_lines():
    """Up to the floor of 5 event-bearing drops is treated as sporadic noise."""
    rows = [
        *(_record(f"good{i}", event="search", user_id="u") for i in range(50)),
        *(_record(f"bad{i}", event="search", params={}, user_id="u") for i in range(4)),
    ]
    result = _drift_check(rows)
    assert result.passed is True


def test_schema_drift_check_fails_on_unrecognized_event_type():
    rows = [
        *(_record(f"good{i}", event="search", user_id="u") for i in range(5)),
        *(
            _record(f"new{i}", payload={"event": "page_view", "timestamp": TS})
            for i in range(6)
        ),
    ]
    result = _drift_check(rows)
    assert result.passed is False
    assert result.severity.value == "ERROR"
    assert "page_view" in result.metadata["unrecognized_event_types"].value
    # the reviewer-facing surfaces name the culprit and its volume
    assert "page_view×6" in result.metadata["dropped_event_values"].value
    assert "page_view×6" in result.description


def test_schema_drift_check_fails_when_known_event_breaks_in_bulk():
    """A field/filter change that breaks all searches trips the tolerance."""
    rows = [
        *(_record(f"good{i}", event="hit", user_id="u") for i in range(20)),
        *(
            _record(
                f"bad{i}",
                event="search",
                params={"name": "t", "page": 1, "perPage": 50, "filters": "oops"},
                user_id="u",
            )
            for i in range(20)
        ),
    ]
    result = _drift_check(rows)
    assert result.passed is False
    assert "search×20" in result.metadata["dropped_event_values"].value


def test_drift_report_is_actionable():
    """One block per event value: dropped/parsed counts, diagnosis, key union, sample."""
    dropped = [
        {"event": "preview", "timestamp": TS, "package": "pudl", "table_name": "x"},
        {"event": "preview", "timestamp": TS, "partition": None, "table_name": "y"},
        {
            "event": "search",
            "timestamp": TS,
            "params": {"name": "t", "page": 1, "perPage": 50, "filters": "oops"},
        },
    ]
    report = _drift_report(
        "2026-06-16",
        total_event_bearing=53,
        dropped=dropped,
        parsed_counts=Counter({"search": 40, "hit": 10}),
    )

    assert "EEL-HOLE SCHEMA DRIFT -- 2026-06-16" in report
    assert "3 of 53 event-bearing payloads (6%)" in report
    assert "preview -- 2 dropped, 0 parsed" in report
    assert "NOT in ALLOWABLE_EVENT_TYPES" in report
    assert "search -- 1 dropped, 40 parsed" in report
    assert "KNOWN event, payload rejected -- params.filters:" in report
    # union of keys across both preview payloads
    assert "package" in report and "partition" in report and "table_name" in report
    assert "params keys seen:" in report
    assert '"filters": "oops"' in report  # a real payload to eyeball
    assert "reprocess partition 2026-06-16" in report


def test_core_eel_hole_logs_emits_blocking_check():
    """The asset yields the drift check alongside its output."""
    raw = pd.DataFrame(
        [
            *(_record(f"good{i}", event="search", user_id="u") for i in range(5)),
            *(
                _record(f"new{i}", payload={"event": "page_view", "timestamp": TS})
                for i in range(6)
            ),
        ]
    )
    result = _check(raw)
    assert result.check_name == EEL_HOLE_SCHEMA_DRIFT_CHECK
    assert result.passed is False
