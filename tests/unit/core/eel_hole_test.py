"""Tests for the eel hole (PUDL Viewer) log transform assets."""

from collections import Counter

import pandas as pd
import pytest
from dagster import AssetCheckResult, Output, build_asset_context

from usage_metrics.core.eel_hole import (
    EEL_HOLE_EVENT_COVERAGE_CHECK,
    EelHoleLogs,
    _core_eel_hole_logs,
    _coverage_report,
    _event_coverage_check,
    payload_is_event,
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
    """Materialize ``_core_eel_hole_logs`` and return its event-coverage check."""
    results = list(_core_eel_hole_logs(ctx or _ctx(), raw))
    return next(r for r in results if isinstance(r, AssetCheckResult))


def _coverage(rows: list[dict], partition_key: str = PARTITION) -> AssetCheckResult:
    """Run ``_event_coverage_check`` on ``rows`` (parsed the way the asset does)."""
    models = [EelHoleLogs(**row).model_dump() for row in rows]
    return _event_coverage_check(_ctx(partition_key), rows, models)


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


# --- payload_is_event: is this a usable structured event? -------------------

_IS_EVENT = [
    pytest.param({"event": "search", "timestamp": TS}, id="minimal-search"),
    pytest.param({"event": "hit", "timestamp": TS, "score": None}, id="hit-null-score"),
    pytest.param(
        {"event": "privacy-policy", "timestamp": TS, "accepted": True}, id="privacy"
    ),
    pytest.param(
        {"event": "preview", "timestamp": TS, "package": "pudl", "table_name": "x"},
        id="new-preview-event",
    ),
    pytest.param(
        {
            "event": "duckdb_other",
            "timestamp": TS,
            "params": {"filters": "[]", "name": "x", "page": "1", "perPage": "10"},
        },
        id="new-duckdb_other-event",
    ),
    pytest.param(
        {"event": "verify-email-failed", "timestamp": TS, "status_code": 500},
        id="verify-email-failed",
    ),
    pytest.param({"event": "search", "timestamp": TS, "params": {}}, id="empty-params"),
    pytest.param(
        {"event": "search", "timestamp": TS, "params": {"name": "t"}},
        id="partial-params",
    ),
    pytest.param(
        {"event": "duckdb_preview", "timestamp": TS, "params": {"table": "x"}},
        id="reshaped-params",
    ),
    pytest.param(
        {"event": "search", "timestamp": TS, "params": _PARAMS_WITH_FILTERS},
        id="filters-list",
    ),
    pytest.param(
        {
            "event": "search",
            "timestamp": TS,
            "params": {"filters": '[{"fieldName": "y", "operation": "between"}]'},
        },
        id="filters-json-string-new-op",
    ),
    pytest.param(
        {"event": "search", "timestamp": TS, "somethingBrandNew": 123},
        id="unknown-extra-key",
    ),
]

_NOT_EVENT = [
    pytest.param({}, id="empty-dict"),
    pytest.param(None, id="none"),
    pytest.param("loading data...", id="string"),
    pytest.param([1, 2, 3], id="list"),
    pytest.param(5, id="int"),
    pytest.param({"timestamp": TS}, id="missing-event"),
    pytest.param({"event": "search"}, id="missing-timestamp"),
    pytest.param({"event": 123, "timestamp": TS}, id="non-string-event"),
    pytest.param({"event": "loading", "timestamp": TS}, id="ignored-noise-event"),
    pytest.param(
        {"event": "Loading prebuilt search index from .idx", "timestamp": TS},
        id="prose-event",
    ),
    pytest.param({"event": "PageView", "timestamp": TS}, id="capitalized-event"),
    pytest.param({"event": "search", "timestamp": "not-a-date"}, id="bad-timestamp"),
    pytest.param(
        {"event": "search", "timestamp": TS, "params": {"filters": "nonsense"}},
        id="filters-not-json",
    ),
    pytest.param(
        {"event": "search", "timestamp": TS, "params": {"filters": "{}"}},
        id="filters-json-not-a-list",
    ),
]


@pytest.mark.parametrize("payload", _IS_EVENT)
def test_payload_is_event_true(payload):
    assert payload_is_event(payload) is True


@pytest.mark.parametrize("payload", _NOT_EVENT)
def test_payload_is_event_false(payload):
    assert payload_is_event(payload) is False


# --- _core_eel_hole_logs ---------------------------------------------------


def test_returns_empty_frame_for_empty_input():
    """An empty raw partition yields an empty DataFrame rather than raising."""
    out = _run(pd.DataFrame())
    assert out.empty


def test_handles_partition_with_no_search_filters():
    """A partition where no search event carries ``params.filters`` transforms fine.

    ``pd.json_normalize`` only emits the ``json_payload_params_filters`` column
    when at least one record has search filters, so the transform must not
    assume that column exists.
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
    assert [col for col in out.columns if "params_filters_" in col]
    assert "json_payload_params_filters" not in out.columns


def test_unknown_event_flows_through_non_fatally():
    """A brand-new event type parses and reaches _core_eel_hole_logs (Option 1)."""
    raw = pd.DataFrame(
        [
            _record("s", event="search", user_id="u"),
            _record(
                "p",
                payload={
                    "event": "preview",
                    "timestamp": TS,
                    "package": "pudl",
                    "table_name": "core_eia860__cooling_equipment",
                },
            ),
        ]
    )

    out = _run(raw)

    assert set(out["insert_id"]) == {"s", "p"}
    row = out.loc[out.insert_id == "p"].iloc[0]
    assert row["event"] == "preview"
    assert row["table_name"] == "core_eia860__cooling_equipment"


@pytest.mark.parametrize(
    "bad_payload",
    [
        pytest.param({}, id="empty-dict"),
        pytest.param({"event": "search"}, id="missing-timestamp"),
        pytest.param({"timestamp": TS}, id="missing-event"),
        pytest.param({"event": "loading", "timestamp": TS}, id="ignored-noise"),
        pytest.param({"event": "Loading prebuilt idx", "timestamp": TS}, id="prose"),
        pytest.param("some app log line", id="string-payload"),
        pytest.param({"event": "search", "timestamp": "nope"}, id="bad-timestamp"),
    ],
)
def test_non_event_payloads_are_dropped(bad_payload):
    """A non-event payload drops that row; the partition still processes."""
    raw = pd.DataFrame(
        [
            _record("good", event="search", user_id="user-1"),
            _record("bad", payload=bad_payload, text_payload=None),
        ]
    )

    out = _run(raw)

    assert list(out["insert_id"]) == ["good"]


def test_partition_with_no_events_returns_empty():
    """A day of nothing but app-noise log lines yields an empty DataFrame."""
    raw = pd.DataFrame(
        [
            _record("a", payload="Starting server on :8080", text_payload="noise"),
            _record("b", payload={}, text_payload="more noise"),
            _record("c", payload=None, text_payload=None),
        ]
    )

    assert _run(raw).empty


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


# --- _event_coverage_check -----------------------------------------------


def test_coverage_check_passes_when_all_events_routed():
    rows = [_record(str(i), event="search", user_id="u") for i in range(5)]
    result = _coverage(rows)
    assert result.passed is True
    assert result.metadata["parsed_events"].value == 5
    assert result.metadata["unrouted_events"].value == "none"


def test_coverage_check_ignores_noise_and_prose_events():
    rows = [
        *(_record(f"s{i}", event="search", user_id="u") for i in range(5)),
        _record("n", payload={"event": "loading", "timestamp": TS}),
        *(
            _record(
                f"log{i}", payload={"event": "Loading prebuilt idx", "timestamp": TS}
            )
            for i in range(6)
        ),
    ]
    result = _coverage(rows)
    assert result.passed is True
    assert result.metadata["non_slug_payloads_nulled"].value == 6


def test_coverage_check_warns_non_fatally_on_unrouted_event():
    """A parsed-but-unrouted event -> WARN (non-blocking), not ERROR."""
    rows = [
        *(_record(f"s{i}", event="search", user_id="u") for i in range(10)),
        *(
            _record(
                f"p{i}",
                payload={
                    "event": "preview",
                    "timestamp": TS,
                    "package": "pudl",
                    "table_name": "x",
                },
            )
            for i in range(903)
        ),
    ]
    result = _coverage(rows)
    assert result.passed is False
    assert result.severity.value == "WARN"  # non-blocking
    assert "preview×903" in result.metadata["unrouted_events"].value
    assert "preview×903" in result.description


def test_coverage_check_errors_when_slug_events_fail_to_parse():
    """Slug events that can't be parsed at all -> ERROR (blocking)."""
    rows = [
        *(_record(f"ok{i}", event="hit", user_id="u") for i in range(20)),
        *(
            _record(f"bad{i}", payload={"event": "search", "timestamp": "not-a-date"})
            for i in range(20)
        ),
    ]
    result = _coverage(rows)
    assert result.passed is False
    assert result.severity.value == "ERROR"
    assert result.metadata["malformed_slug_events"].value == 20


def test_coverage_report_is_actionable():
    """One block per unrouted event: count, key union, sample; plus routed summary."""
    unrouted = {
        "preview": [
            {"event": "preview", "timestamp": TS, "package": "pudl", "table_name": "x"},
            {"event": "preview", "timestamp": TS, "partition": None, "table_name": "y"},
        ],
        "duckdb_other": [
            {
                "event": "duckdb_other",
                "timestamp": TS,
                "params": {"name": "x", "page": 1},
            },
        ],
    }
    report = _coverage_report(
        "2026-06-16",
        routed=Counter({"search": 400, "duckdb_csv": 2}),
        unrouted=unrouted,
        malformed=[],
    )

    assert "EEL-HOLE EVENT COVERAGE -- 2026-06-16" in report
    assert "NOT routed to a core_eel_hole_* table" in report
    assert "preview -- 2 events" in report
    assert "duckdb_other -- 1 events" in report
    assert "package" in report and "partition" in report and "table_name" in report
    assert "params keys seen: name, page" in report
    assert "Routed OK: search×400, duckdb_csv×2" in report
    assert "To route a new event" in report


def test_coverage_report_shows_malformed_events():
    report = _coverage_report(
        "2026-06-16",
        routed=Counter({"hit": 20}),
        unrouted={},
        malformed=[{"event": "search", "timestamp": "not-a-date"}],
    )
    assert "FAILED to parse" in report
    assert "search -- 1 events -- timestamp:" in report


def test_core_eel_hole_logs_emits_coverage_check():
    """The asset yields the coverage check alongside its output."""
    raw = pd.DataFrame(
        [_record(f"s{i}", event="search", user_id="u") for i in range(3)]
    )
    result = _check(raw)
    assert result.check_name == EEL_HOLE_EVENT_COVERAGE_CHECK
    assert result.passed is True
