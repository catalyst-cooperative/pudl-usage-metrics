"""Transform data from data.catalyst.coop (the PUDL Data Viewer) logs."""

import datetime
import json
import math
import os
import re
from collections import Counter
from collections.abc import Iterator
from typing import Annotated, Any, Literal, NamedTuple, cast

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Output,
    asset,
)
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    ValidationError,
    field_validator,
)
from pydantic.alias_generators import to_camel

EVENT_FIELDS: dict[str, frozenset[str]] = {
    "search": frozenset({"url", "query"}),
    "log_in": frozenset({"next_url", "is_new_user"}),
    "preview": frozenset({"package", "table_name", "partition"}),
    "duckdb_preview": frozenset({"url", "params"}),
    "duckdb_csv": frozenset({"url", "params"}),
    "duckdb_other": frozenset({"url", "params"}),
    "privacy-policy": frozenset({"accepted", "outreach", "newsletter"}),
    "verify-email-requested": frozenset(),
    "verify-email-failed": frozenset({"status_code"}),
    "refresh-email-verification-failed": frozenset({"status_code"}),
}
"""The events that have a ``core_eel_hole_*`` table, and the fields the Eel Hole always
logs with each.

A field may be null but must be present, and no other field may appear: either means
the Eel Hole's logging changed, and fails the coverage check. An event not listed here is
reported by the coverage check as unrouted, so add it here together with the table that
persists it."""

ALWAYS_LOGGED_FIELDS = frozenset({"user_id", "user_domain"})
"""Fields the Eel Hole logs with every event, null for anonymous visitors."""

ROUTED_EVENT_TYPES: frozenset[str] = frozenset(EVENT_FIELDS)
"""``jsonPayload.event`` values that have a persisted ``core_eel_hole_*`` table.

Any slug event parses, but one not in this set is not written anywhere and is
reported by the ``eel_hole_event_coverage`` check."""

EEL_HOLE_EVENT_COVERAGE_CHECK = "eel_hole_event_coverage"
"""The name of the coverage check, shared by its spec and every result it returns so
they can't drift apart."""

KNOWN_PARAM_KEYS: frozenset[str] = frozenset({"filters", "name", "page", "perPage"})
"""The query-string keys the Eel Hole's AG Grid sends to ``/api/duckdb``.

The Eel Hole logs each request's query string. The ``/api/duckdb`` endpoint requires
login, so a logged-in request with any other key means the Eel Hole's request structure
changed, and fails the coverage check, so we are forced to update our expectations here.
"""

PARAM_COLUMNS: tuple[str, ...] = (
    "params_name",
    "params_page",
    "params_per_page",
    "params_filters",
)
"""``KNOWN_PARAM_KEYS`` as columns of ``core_eel_hole_{previews,downloads,duckdb_other}``."""

IGNORED_EVENT_TYPES: frozenset[str] = frozenset({"loading"})
"""Slug events that are known Eel Hole noise (e.g. ``loading``). Dropped without
counting as a coverage gap. Prose operational log lines are filtered by shape."""

DUCKDB_EVENT_PREFIX = "duckdb_"
"""Prefix of the ``/api/duckdb`` request events (``duckdb_preview``, ``duckdb_csv``,
``duckdb_other``)."""


_EVENT_SLUG = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
"""What a real ``jsonPayload.event`` looks like: a lower-case identifier of at most 64
characters (a loose sanity bound).

The Eel Hole uses structlog, where ``event`` is the log message: analytics events
are slugs (``search``, ``preview``, ``duckdb_csv``), operational log lines are
prose (``"Loading prebuilt search index from ..."``). We only keep slugs."""

_EVENT_SLUG_ANY_CASE = re.compile(_EVENT_SLUG.pattern, re.IGNORECASE)
"""A slug ignoring case. A non-slug ``event`` that matches this differs from a real
slug only by case (``Preview``, ``DuckDB_Preview``), so it is almost certainly a
renamed analytics event rather than prose. Those are reported, not silently dropped."""


def json_string_to_list(value: Any):
    """Convert a list formatted into a string into a list datatype."""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            raise ValueError("Parsed value is not a list")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")
    return value


ALLOWABLE_FIELD_TYPES = Literal["text", "number", "bigint", "date"]
"""The AG Grid ``filterType`` values our columns can produce.

A value outside this list fails the coverage check on purpose: add it once it is
understood."""

ALLOWABLE_OPERATIONS = Literal[
    "equals",
    "contains",
    "greaterThan",
    "greaterThanOrEqual",
    "lessThan",
    "lessThanOrEqual",
    "notBlank",
    "startsWith",
    "endsWith",
    "notEqual",
    "notContains",
    "inRange",
    "blank",
    "false",
    "true",
]
"""The AG Grid filter operations, in AG Grid's own camelCase spelling, which the output
keeps as is. As with ``ALLOWABLE_FIELD_TYPES``, a value outside this list fails the
coverage check."""


class DuckDBFilters(BaseModel):
    """A filter in AG Grid's filter format, as the Eel Hole logs it.

    Validated strictly: a value outside ``ALLOWABLE_FIELD_TYPES`` or
    ``ALLOWABLE_OPERATIONS`` fails the coverage check, and likely means AG Grid added
    one.
    """

    field_name: str
    field_type: ALLOWABLE_FIELD_TYPES
    operation: ALLOWABLE_OPERATIONS
    value: str | int | float | bool | None = None
    value_to: str | int | float | bool | None = None

    # AG Grid sends camelCase keys; the attributes are snake_case, so alias them.
    model_config = ConfigDict(alias_generator=to_camel)


def filters_to_json(filters: Any) -> str | None:
    """Serialize a query's validated filters as a JSON array, or None if it has none."""
    if not isinstance(filters, list) or not filters:
        return None
    return json.dumps(filters, separators=(",", ":"))


class DuckDBParams(BaseModel):
    """Query-string parameters of a ``/api/duckdb`` request.

    Only ``KNOWN_PARAM_KEYS`` are allowed: another key means the Eel Hole's requests
    changed, and fails the coverage check. Fields are optional because a request
    missing some of them is only logged.
    """

    filters: Annotated[
        list[DuckDBFilters] | None, BeforeValidator(json_string_to_list)
    ] = None
    name: str | None = None
    page: int | None = None
    per_page: int | None = None

    # AG Grid sends camelCase keys; the attributes are snake_case, so alias them.
    model_config = ConfigDict(alias_generator=to_camel, extra="forbid")


class JsonPayload(BaseModel):
    """A structlog JSON log line from the Eel Hole.

    Deliberately permissive: only ``event`` and ``timestamp`` are required, so a new
    event type or payload shape flows through to ``_core_eel_hole_logs`` and is
    reported by the coverage check instead of crashing the partition.
    """

    event: str
    timestamp: datetime.datetime
    user_id: str | None = None
    user_domain: str | None = None
    # 'search'
    query: str | None = None
    url: str | None = None
    # 'preview' -- the /preview/<package>/<table_name> page view
    package: str | None = None
    table_name: str | None = None
    partition: str | None = None  # renamed data_partition in the output table
    # 'duckdb_preview' / 'duckdb_csv' / 'duckdb_other' -- raw request args
    params: DuckDBParams | None = None
    # 'privacy-policy'
    accepted: bool | None = None
    newsletter: bool | None = None
    outreach: bool | None = None
    # 'verify-email-failed'
    status_code: int | None = None
    # 'log_in'
    next_url: str | None = None
    is_new_user: bool | None = None

    # The Eel Hole logs its own fields in snake_case, so no aliases here, unlike the
    # models for what GCP and AG Grid send. Only the spelling it logs is accepted.

    @field_validator("*", mode="before")
    def replace_na_with_none(cls, value):  # noqa: N805
        """To successfully validate string dtypes, convert NaNs to None."""
        if isinstance(value, float) and math.isnan(value):
            return None
        return value


def is_anonymous_duckdb_event(payload: dict) -> bool:
    """Whether ``payload`` is a ``/api/duckdb`` request with no logged-in user.

    Anonymous visitors can't use the data grid, so these are bot and scraper requests
    (hand-built probes). They are ignored and only counted.
    """
    event = payload.get("event")
    return (
        isinstance(event, str)
        and event.startswith(DUCKDB_EVENT_PREFIX)
        and not payload.get("user_id")
    )


def payload_is_event(value: Any) -> bool:
    """Whether a raw ``jsonPayload`` is a usable structured event.

    True for a dict whose ``event`` is a slug (see ``_EVENT_SLUG``) not in
    ``IGNORED_EVENT_TYPES``, that isn't an anonymous ``duckdb_*`` request, and that
    parses as a ``JsonPayload``. This filters out operational log lines (prose
    ``event``), bot requests and structurally broken records. Unknown event types
    still flow through and are reported as coverage gaps by ``_event_coverage_check``.
    """
    if not isinstance(value, dict):
        return False
    event = value.get("event")
    if not isinstance(event, str) or not _EVENT_SLUG.match(event):
        return False
    if event in IGNORED_EVENT_TYPES or is_anonymous_duckdb_event(value):
        return False
    try:
        JsonPayload.model_validate(value)
    except ValidationError:
        return False
    return True


class EelHoleLogs(BaseModel):
    """The fields of a GCP Cloud Run log entry that we use.

    Everything else GCP sends (``labels``, ``resource``, ``logName``,
    ``receiveTimestamp``) is ignored, so a change to that metadata can't break a run.
    A change to the fields below fails validation.
    """

    insert_id: str
    json_payload: JsonPayload | None = None
    timestamp: datetime.datetime

    @field_validator("*", mode="before")
    def replace_na_with_none(cls, value):  # noqa: N805
        """To successfully validate string dtypes, convert NaNs to None."""
        if isinstance(value, float) and math.isnan(value):
            return None
        return value

    @field_validator("json_payload", mode="before")
    def drop_non_event_payload(cls, value):  # noqa: N805
        """Null a JSON payload that isn't a usable event.

        Nulling it here -- rather than letting ``EelHoleLogs`` validation raise
        and kill the whole partition -- lets the row fall out downstream. See
        ``payload_is_event``.
        """
        if isinstance(value, JsonPayload) or payload_is_event(value):
            return value
        return None

    # GCP names its log-entry keys in camelCase; alias them to the snake_case fields.
    model_config = ConfigDict(alias_generator=to_camel)


def _missing_fields(payload: dict) -> frozenset[str]:
    """The fields the Eel Hole always logs with this event that ``payload`` lacks."""
    expected = EVENT_FIELDS.get(str(payload.get("event")))
    if expected is None:
        return frozenset()
    return (expected | ALWAYS_LOGGED_FIELDS) - payload.keys()


def _unexpected_fields(payload: dict) -> frozenset[str]:
    """The fields in ``payload`` that the Eel Hole isn't expected to log with its event."""
    expected = EVENT_FIELDS.get(str(payload.get("event")))
    if expected is None:
        return frozenset()
    known = expected | ALWAYS_LOGGED_FIELDS | {"event", "timestamp"}
    return frozenset(payload.keys() - known)


def _payload_error(payload: Any) -> str:
    """One-line summary of the first reason ``payload`` is malformed."""
    try:
        JsonPayload.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        loc = ".".join(str(part) for part in first["loc"]) or "(root)"
        return f"{loc}: {first['msg']}"
    if isinstance(payload, dict):
        problems = []
        if missing := _missing_fields(payload):
            problems.append(f"missing fields: {', '.join(sorted(missing))}")
        if unexpected := _unexpected_fields(payload):
            problems.append(f"unexpected fields: {', '.join(sorted(unexpected))}")
        if problems:
            return "; ".join(problems)
    return "(now valid)"


def _keys_seen(payloads: list[dict], path: str | None = None) -> str:
    """Sorted union of keys across ``payloads`` (or their ``params`` sub-dicts)."""
    dicts = (
        payloads
        if path is None
        else [p[path] for p in payloads if isinstance(p.get(path), dict)]
    )
    return ", ".join(sorted({key for d in dicts for key in d})) or "(none)"


def _n_pct(n: int, total: int) -> str:
    """``"903 (66%)"`` -- absolute count and its share of ``total`` events."""
    return f"{n} ({n / total:.0%})" if total else f"{n}"


def _event_summary(counts: Counter, total: int) -> str:
    """``"preview×903 (66%), duckdb_other×4 (0%)"`` for a Counter of event -> n."""
    parts = [f"{event}×{_n_pct(n, total)}" for event, n in counts.most_common()]
    return ", ".join(parts) or "none"


def _coverage_report(
    partition_key: str,
    total: int,
    routed: Counter,
    unrouted: dict[str, list[dict]],
    malformed: list[dict],
    case_variants: Counter | None = None,
) -> str:
    """Summarize the coverage gap, with each count as ``n (pct)`` of ``total`` events.

    One block per unrouted or malformed event name: its count, the keys seen across
    its payloads, and one sample.
    """
    lines = [f"EEL-HOLE EVENT COVERAGE -- {partition_key} ({total} slug events)"]

    if unrouted:
        unrouted_n = sum(len(v) for v in unrouted.values())
        lines += [
            "",
            (
                "  Parsed but NOT routed to a core_eel_hole_* table -- "
                f"{_n_pct(unrouted_n, total)} of events"
            ),
            "  (these are dropped before the parquet outputs / dashboards):",
        ]
        for event_value, payloads in sorted(
            unrouted.items(), key=lambda item: -len(item[1])
        ):
            lines += [
                "",
                f"  {event_value} -- {_n_pct(len(payloads), total)}",
                f"    keys seen:        {_keys_seen(payloads)}",
            ]
            if any(isinstance(p.get("params"), dict) for p in payloads):
                lines.append(f"    params keys seen: {_keys_seen(payloads, 'params')}")
            sample = json.dumps(payloads[0], default=str, sort_keys=True)[:500]
            lines.append(f"    sample: {sample}")

    if malformed:
        grouped: dict[str, list[dict]] = {}
        for payload in malformed:
            grouped.setdefault(str(payload["event"]), []).append(payload)
        lines += [
            "",
            (
                "  Slug events that FAILED to parse or have missing or unexpected fields -- "
                f"{_n_pct(len(malformed), total)} of events"
            ),
            "  (the eel-hole log format may have changed -- this fails the check):",
        ]
        for event_value, payloads in sorted(
            grouped.items(), key=lambda item: -len(item[1])
        ):
            sample = json.dumps(payloads[0], default=str, sort_keys=True)[:500]
            lines += [
                "",
                (
                    f"  {event_value} -- {_n_pct(len(payloads), total)} -- "
                    f"{_payload_error(payloads[0])}"
                ),
                f"    sample: {sample}",
            ]

    if case_variants:
        lines += [
            "",
            "  Events DROPPED because their name isn't a lower-case slug, though it",
            "  differs from one only by case (a renamed analytics event?):",
            f"  {_event_summary(case_variants, total)}",
            "  eel-hole event names must match [a-z][a-z0-9_-]{0,63}: fix the name",
            "  in eel-hole, or handle the new naming here.",
        ]

    lines += [
        "",
        f"  Routed OK: {_event_summary(routed, total)}",
        "",
        "  To route a new event: add a core_eel_hole_<name> asset in",
        "  usage_metrics.core.eel_hole, a pyarrow schema in usage_metrics.models,",
        "  and add it to ROUTED_EVENT_TYPES; then backfill.",
    ]
    return "\n".join(lines)


def _strict_coverage() -> bool:
    """Whether a coverage gap blocks the run: an ERROR in ``prod``, a WARN elsewhere."""
    return os.getenv("METRICS_PROD_ENV", "local") == "prod"


def _missing_params_metadata(
    context: AssetExecutionContext, rows: list[dict], models: list[dict]
) -> dict[str, Any]:
    """Warn about kept ``duckdb_*`` requests that lack some of ``KNOWN_PARAM_KEYS``.

    An unexpected key results in an error, but we allow and warn on a request with missing keys
    since we have occasionally manually constructed ``/api/duckdb?`` URLs to test whether the
    API endpoint is visible to anonymous users, and such requests shouldn't fail the partition.
    """
    missing = 0
    for row, model in zip(rows, models, strict=True):
        payload = row.get("jsonPayload")
        if model["json_payload"] is None or not isinstance(payload, dict):
            continue
        if str(payload["event"]).startswith(DUCKDB_EVENT_PREFIX):
            params = payload.get("params")
            missing += not isinstance(params, dict) or bool(
                KNOWN_PARAM_KEYS - params.keys()
            )
    if missing:
        context.log.warning(
            f"{context.partition_key}: {missing} logged-in /api/duckdb requests lack "
            f"some of {sorted(KNOWN_PARAM_KEYS)}."
        )
    return {"duckdb_events_missing_params": missing}


class _EventClasses(NamedTuple):
    """Slug events sorted by ``_classify_events``, plus counts of the ignored ones."""

    routed: Counter
    unrouted: dict[str, list[dict]]
    malformed: list[dict]
    non_slug_nulled: int
    anonymous_duckdb: int
    case_variants: Counter  # non-slug events that differ from a slug only by case


def _classify_events(rows: list[dict], models: list[dict]) -> _EventClasses:
    """Sort events into routed, unrouted and malformed; count the ignored ones."""
    routed: Counter = Counter()
    unrouted: dict[str, list[dict]] = {}
    malformed: list[dict] = []
    case_variants: Counter = Counter()
    non_slug_nulled = anonymous_duckdb = 0

    for row, model in zip(rows, models, strict=True):
        payload = row.get("jsonPayload")
        if not isinstance(payload, dict) or not isinstance(payload.get("event"), str):
            continue
        event = payload["event"]
        if event in IGNORED_EVENT_TYPES:
            continue
        if not _EVENT_SLUG.match(event):
            if model["json_payload"] is None:
                non_slug_nulled += 1
                if _EVENT_SLUG_ANY_CASE.match(event):
                    case_variants[event] += 1
        elif is_anonymous_duckdb_event(payload):
            anonymous_duckdb += 1
        elif (
            model["json_payload"] is None
            or _missing_fields(payload)
            or _unexpected_fields(payload)
        ):
            malformed.append(payload)
        elif event in ROUTED_EVENT_TYPES:
            routed[event] += 1
        else:
            unrouted.setdefault(event, []).append(payload)
    return _EventClasses(
        routed, unrouted, malformed, non_slug_nulled, anonymous_duckdb, case_variants
    )


def _event_coverage_check(
    context: AssetExecutionContext, rows: list[dict], models: list[dict]
) -> AssetCheckResult:
    """Report events we can't persist, and log the full ``_coverage_report``.

    * Parsed events not in ``ROUTED_EVENT_TYPES``, and event names that differ from a
      slug only by case (``Preview``): WARN outside ``prod``, ERROR in ``prod``.
    * An event that fails to parse, lacks a field in ``EVENT_FIELDS``, carries one that
      isn't listed there, or is a logged-in ``duckdb_*`` request outside AG Grid's
      vocabulary or the known params: ERROR, even a single one. We control both the
      logger and this parser, so any such event means one of them changed.

    Anonymous ``duckdb_*`` requests (bots) are ignored, counted and left out of the
    totals.
    """
    partition_key = context.partition_key
    classes = _classify_events(rows, models)
    routed, unrouted, malformed = classes.routed, classes.unrouted, classes.malformed
    anonymous_duckdb, case_variants = classes.anonymous_duckdb, classes.case_variants

    total = (
        sum(routed.values()) + sum(len(v) for v in unrouted.values()) + len(malformed)
    )
    if anonymous_duckdb:
        context.log.info(
            f"{partition_key}: ignored {anonymous_duckdb} anonymous /api/duckdb "
            "events (bot or scraper traffic)."
        )
    unrouted_counts = Counter({e: len(v) for e, v in unrouted.items()})
    unrouted_n = sum(unrouted_counts.values())

    metadata = {
        "total_slug_events": total,
        "parsed_events": total - len(malformed),
        "routed_events": _event_summary(routed, total),
        "unrouted_events": _event_summary(unrouted_counts, total),
        "unrouted_pct": (round(100 * unrouted_n / total, 1) if total else 0.0),
        "malformed_slug_events": len(malformed),
        "non_slug_payloads_nulled": classes.non_slug_nulled,
        "case_variant_events_dropped": sum(case_variants.values()),
        "anonymous_duckdb_events_ignored": anonymous_duckdb,
        **_missing_params_metadata(context, rows, models),
    }

    if malformed:
        report = _coverage_report(
            partition_key, total, routed, unrouted, malformed, case_variants
        )
        context.log.error(report)
        return AssetCheckResult(
            check_name=EEL_HOLE_EVENT_COVERAGE_CHECK,
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=(
                f"{partition_key}: {_n_pct(len(malformed), total)} eel-hole events "
                "failed to parse or have missing or unexpected fields -- the log format, AG Grid's "
                "vocabulary or the Eel Hole's request params changed. If AG Grid added "
                "a value, add it to ALLOWABLE_FIELD_TYPES / ALLOWABLE_OPERATIONS. See "
                "the 'report' metadata below for the full breakdown."
            ),
            metadata={**metadata, "report": report},
        )

    if unrouted or case_variants:
        strict = _strict_coverage()
        report = _coverage_report(
            partition_key, total, routed, unrouted, malformed, case_variants
        )
        (context.log.error if strict else context.log.warning)(report)
        gaps = []
        if unrouted:
            gaps.append(
                f"NOT persisting {_n_pct(unrouted_n, total)} of eel-hole events "
                f"({metadata['unrouted_events']}) -- add a core_eel_hole_* table"
            )
        if case_variants:
            gaps.append(
                f"dropped {sum(case_variants.values())} events whose names differ "
                f"from a lower-case slug only by case ({', '.join(case_variants)})"
            )
        return AssetCheckResult(
            check_name=EEL_HOLE_EVENT_COVERAGE_CHECK,
            passed=False,
            severity=AssetCheckSeverity.ERROR if strict else AssetCheckSeverity.WARN,
            description=(
                f"{partition_key}: "
                + "; ".join(gaps)
                + ("." if strict else " (non-fatal).")
                + " See the 'report' metadata below for the full breakdown."
            ),
            metadata={**metadata, "report": report},
        )

    return AssetCheckResult(
        check_name=EEL_HOLE_EVENT_COVERAGE_CHECK,
        passed=True,
        severity=AssetCheckSeverity.WARN,
        description=f"{partition_key}: all parsed eel-hole events are routed to a table.",
        metadata=metadata,
    )


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "eel_hole"},
    check_specs=[
        AssetCheckSpec(
            name=EEL_HOLE_EVENT_COVERAGE_CHECK,
            asset="_core_eel_hole_logs",
            blocking=True,
        )
    ],
)
def _core_eel_hole_logs(
    context: AssetExecutionContext,
    raw_eel_hole_logs: pd.DataFrame,
) -> Iterator[Output[pd.DataFrame] | AssetCheckResult]:
    """Transform data.catalyst.coop (the PUDL Data Viewer) logs."""
    context.log.info(f"Processing data for {context.partition_key}")

    if raw_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        yield Output(pd.DataFrame())
        yield _event_coverage_check(context, [], [])
        return

    # Flatten the many nested columns and coerce them into the expected class.
    # to_dict()'s declared return type keys columns as Hashable (the general case
    # for a DataFrame index/columns), but our columns are always strings.
    rows = cast("list[dict[str, Any]]", raw_eel_hole_logs.to_dict(orient="records"))
    # The payload's own timestamp is the least complete, so we keep the log entry's
    # ``timestamp`` (https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry).
    models = [
        EelHoleLogs(**row).model_dump(exclude={"json_payload": {"timestamp"}})
        for row in rows
    ]

    converted_df = pd.json_normalize(models, sep="_")
    # Drop the columns for nested structures that json_normalize exploded. Whether
    # they exist depends on the day's data (a partition with no parseable payloads
    # never expands them), not on GCP's schema.
    converted_df = converted_df.drop(
        columns=["json_payload", "json_payload_params"], errors="ignore"
    )

    # If every record in the partition had its payload dropped (e.g. a day of
    # nothing but app noise, or every search's params happened to be malformed),
    # none of the json_payload_* columns exist at all. Synthesize the (all-null)
    # event columns the rest of this transform selects, so it doesn't KeyError.
    if "json_payload_event" not in converted_df.columns:
        for field in JsonPayload.model_fields:
            if field not in ("timestamp", "params"):
                converted_df[f"json_payload_{field}"] = pd.NA

    # The filters are a list of dictionaries of unbounded length, so rather than
    # spreading them over numbered columns we keep each query's list as one JSON
    # string. This column only exists if at least one record in the partition had
    # search filters, so we have to check before trying to process it.
    if "json_payload_params_filters" in converted_df.columns:
        converted_df["json_payload_params_filters"] = converted_df[
            "json_payload_params_filters"
        ].map(filters_to_json)

    # Remove json_payload from the column names
    converted_df.columns = converted_df.columns.str.replace("json_payload_", "")

    # A partition with no duckdb requests (or none with filters) has none of these
    # columns; the tables that select them expect them, null.
    for column in PARAM_COLUMNS:
        if column not in converted_df.columns:
            converted_df[column] = pd.NA

    # Handle mixed none types in the URL column
    converted_df["url"] = converted_df["url"].replace({None: pd.NA})

    # Drop any remaining rows where there is no event
    # These are some logs that are just about the app itself running.
    converted_df = converted_df.loc[converted_df.event.notnull(), :]

    context.log.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} environment.")

    # Add a session ID for users
    # Increment the session ID if a user has been inactive for 30 min or more.
    def create_session_id(timestamp):
        return timestamp.diff().gt(pd.Timedelta("30min")).cumsum()

    if converted_df.user_id.notnull().any():  # If the data contains user IDs
        session_ids = (
            converted_df.set_index("insert_id")
            .groupby("user_id")["timestamp"]
            .apply(create_session_id)
            + 1
        )

        # Because we process these as partitions, let's make the session_id a concatenation
        # of the weekly partition and the unique session ID.
        # This will take the format 2025-08-31-s1
        session_ids = context.partition_key + "-s" + session_ids.astype(str)

        session_ids = (
            session_ids.reset_index()
            .rename(columns={"timestamp": "session_id"})
            .drop(columns="user_id")
        )

        converted_df = converted_df.merge(
            session_ids, how="left", on="insert_id", validate="1:1"
        )
    else:
        converted_df["session_id"] = pd.NA

    yield Output(converted_df.reset_index(drop=True))
    yield _event_coverage_check(context, rows, models)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_log_ins(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of log-in events from eel-hole logs.

    Logged when a user returns from Auth0 to the Eel Hole after logging in.
    """
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    login_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "log_in"]
    login_df = login_df.loc[
        :,
        [
            "insert_id",
            "user_id",
            "user_domain",
            "timestamp",
            "next_url",
            "is_new_user",
            "session_id",
        ],
    ]

    return login_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_searches(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of search events from eel-hole logs."""
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    search_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "search"]
    search_df = search_df.loc[
        :,
        [
            "insert_id",
            "user_id",
            "user_domain",
            "timestamp",
            "query",
            "url",
            "session_id",
        ],
    ]

    return search_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_previews(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of DuckDB preview requests from eel-hole logs."""
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    preview_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "duckdb_preview"]
    preview_df = preview_df.loc[
        :,
        ["insert_id", "user_id", "user_domain", "timestamp", "url", "session_id"]
        + list(PARAM_COLUMNS),
    ]

    return preview_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_downloads(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of DuckDB download requests from eel-hole logs."""
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    download_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "duckdb_csv"]
    download_df = download_df.loc[
        :,
        ["insert_id", "user_id", "user_domain", "timestamp", "url", "session_id"]
        + list(PARAM_COLUMNS),
    ]

    return download_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_duckdb_other(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of DuckDB query events that are neither previews nor downloads.

    /api/duckdb logs duckdb_preview or duckdb_csv only for the grid's two fixed
    ``perPage`` sizes; any other size lands here, kept apart from those two tables.
    """
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    other_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "duckdb_other"]
    other_df = other_df.loc[
        :,
        ["insert_id", "user_id", "user_domain", "timestamp", "url", "session_id"]
        + list(PARAM_COLUMNS),
    ]

    return other_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_table_views(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of requests to table preview pages from eel-hole logs.

    Logged for every GET to /preview/<package>/<table_name>[/<partition>] before the
    table is looked up, so it includes anonymous visitors, bots, and names that
    aren't tables. ``user_id`` is null for anonymous requests.
    """
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    views_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "preview"]
    views_df = views_df.rename(columns={"partition": "data_partition"})
    views_df = views_df.loc[
        :,
        [
            "insert_id",
            "user_id",
            "user_domain",
            "timestamp",
            "package",
            "table_name",
            "data_partition",
            "session_id",
        ],
    ]

    return views_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_user_settings_updates(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of user setting updates."""
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    settings_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "privacy-policy"]
    settings_df = settings_df.loc[
        :,
        [
            "insert_id",
            "user_id",
            "user_domain",
            "timestamp",
            "accepted",
            "newsletter",
            "outreach",
        ],
    ]

    # If user_id is none, there shouldn't really be a log here.
    # Drop these weirdo records.
    settings_df = settings_df.loc[settings_df.user_id.notnull()]

    return settings_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_verify_email_requests(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of email-verification prompts sent to a logged-in user."""
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    requests_df = _core_eel_hole_logs[
        _core_eel_hole_logs.event == "verify-email-requested"
    ]
    requests_df = requests_df.loc[
        :, ["insert_id", "user_id", "user_domain", "timestamp"]
    ]

    return requests_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_verify_email_failures(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of failed attempts to send/request an email-verification.

    Auth0 rejected our request to send the user a verification email --
    either fetching the management API token or the send-email call itself
    failed. status_code is Auth0's HTTP response code.
    """
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    failures_df = _core_eel_hole_logs[
        _core_eel_hole_logs.event == "verify-email-failed"
    ]
    failures_df = failures_df.loc[
        :, ["insert_id", "user_id", "user_domain", "timestamp", "status_code"]
    ]

    return failures_df.reset_index(drop=True)


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "eel_hole"},
)
def core_eel_hole_email_verification_refresh_failures(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of failed attempts to refresh a user's email-verification state.

    Distinct from core_eel_hole_verify_email_failures: this is Auth0 rejecting
    our lookup of the user's *current* verification status (triggered when the
    Eel Hole polls to see if a user has clicked the verification link yet), not
    a failure to send the email in the first place. status_code is Auth0's
    HTTP response code.
    """
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    failures_df = _core_eel_hole_logs[
        _core_eel_hole_logs.event == "refresh-email-verification-failed"
    ]
    failures_df = failures_df.loc[
        :, ["insert_id", "user_id", "user_domain", "timestamp", "status_code"]
    ]

    return failures_df.reset_index(drop=True)
