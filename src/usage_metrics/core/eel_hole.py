"""Transform data from viewer.catalyst.coop logs."""

import datetime
import json
import math
import os
import re
from collections import Counter
from collections.abc import Iterator
from typing import Annotated, Any
from urllib.parse import urlsplit

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

ROUTED_EVENT_TYPES: frozenset[str] = frozenset(
    {"search", "hit", "duckdb_preview", "duckdb_csv", "privacy-policy"}
)
"""``jsonPayload.event`` values that have a persisted ``core_eel_hole_*`` table.

The models below are deliberately permissive: any slug event parses and flows
into ``_core_eel_hole_logs``. An event *not* in this set parses fine but isn't
written anywhere -- a coverage gap the ``eel_hole_event_coverage`` check
surfaces (non-fatally). Add an entry here only alongside a new downstream table.
(``log_in`` is synthesized from the ``/callback`` request log, not a payload.)"""

EEL_HOLE_EVENT_COVERAGE_CHECK = "eel_hole_event_coverage"

IGNORED_EVENT_TYPES: frozenset[str] = frozenset({"loading"})
"""Slug ``jsonPayload.event`` values that are known viewer noise, not user
events (``loading`` is a client-side "still loading" marker). These are dropped
without counting as a coverage gap. eel-hole's non-slug operational log lines
(``event`` is a prose message) are filtered separately, by shape."""

SCHEMA_DRIFT_TOLERANCE = 0.01
"""Fraction of events that may be slug-but-unparseable (bad/missing timestamp,
non-string event) before ``_event_coverage_check`` fails the partition as ERROR
(floor of 5). That means the eel-hole log *format* changed, not just its event
vocabulary."""


_EVENT_SLUG = re.compile(r"[a-z][a-z0-9_-]{0,40}\Z")
"""What a real ``jsonPayload.event`` looks like (a lower-case identifier).

eel-hole uses structlog, where ``event`` is the log message: analytics events
are slugs (``search``, ``preview``, ``duckdb_csv``), operational log lines are
prose (``"Loading prebuilt search index from ..."``). We only keep slugs."""


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


class DuckDBParams(BaseModel):
    """Loose container for a duckdb query's request params.

    eel-hole logs these as ``dict(request.args)`` (see its ``/api/duckdb``
    route), so the keys vary by request and change over time. Kept permissive --
    everything optional, extra keys allowed -- so a new shape doesn't drop the
    event. ``json_string_to_list`` still normalizes ``filters`` from its
    JSON-string form so the downstream filter split keeps working. Filter dicts
    themselves are not validated here; eel-hole's own ``Filter`` model
    (``field_type``/``operation`` as free ``str``, plus ``value_to``) is the
    reference.
    """

    filters: Annotated[
        list[dict[str, Any]] | None, BeforeValidator(json_string_to_list)
    ] = None
    name: str | None = None
    page: int | None = None
    per_page: int | None = None

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, extra="allow"
    )


class JsonPayload(BaseModel):
    """A structlog JSON log line from the viewer.

    Deliberately permissive: ``event`` is any string (eel-hole uses the log
    message as the event name), unknown ``params`` shapes are accepted as-is,
    and only ``event`` + ``timestamp`` are required. A new event type or a
    changed payload shape therefore flows through to ``_core_eel_hole_logs``
    and shows up as a coverage gap rather than being dropped or crashing the
    partition.
    """

    event: str
    timestamp: datetime.datetime
    user_id: str | None = None
    user_domain: str | None = None
    # 'search' / 'hit'
    name: str | None = None
    query: str | None = None
    score: float | None = None
    tags: str | None = None
    url: str | None = None
    # 'preview' -- the /preview/<package>/<table_name> page view
    package: str | None = None
    table_name: str | None = None
    partition: str | None = None
    # 'duckdb_preview' / 'duckdb_csv' / 'duckdb_other' -- raw request args
    params: DuckDBParams | None = None
    # 'privacy-policy'
    accepted: bool | None = None
    newsletter: bool | None = None
    outreach: bool | None = None
    # 'verify-email-failed'
    status_code: int | None = None

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    @field_validator("*", mode="before")
    def replace_na_with_none(cls, value, info):  # noqa: N805
        """To successfully validate string dtypes, convert NaNs to None."""
        if info.field_name == "score":
            return value  # Skip NaN coercion for the 'score' field
        if isinstance(value, float) and math.isnan(value):
            return None
        return value


def payload_is_event(value: Any) -> bool:
    """Whether a raw ``jsonPayload`` is a usable structured event.

    True for a dict whose ``event`` is a slug (see ``_EVENT_SLUG``) not in
    ``IGNORED_EVENT_TYPES``, that parses as a ``JsonPayload``. This filters out
    eel-hole's operational log lines (prose ``event``) and structurally broken
    records. It deliberately does *not* filter on whether we recognize the event
    type or its payload shape -- unknown events flow through and are surfaced as
    coverage gaps by ``_event_coverage_check``.
    """
    if not isinstance(value, dict):
        return False
    event = value.get("event")
    if not isinstance(event, str) or not _EVENT_SLUG.match(event):
        return False
    if event in IGNORED_EVENT_TYPES:
        return False
    try:
        JsonPayload.model_validate(value)
    except ValidationError:
        return False
    return True


class EelHoleLogs(BaseModel):
    """Expected format of eel hole logs."""

    insert_id: str
    json_payload: JsonPayload | None = None
    labels: dict[str, str]
    log_name: str
    receive_timestamp: datetime.datetime
    resource: dict[str, str | dict[str, str]]
    timestamp: datetime.datetime
    text_payload: str | None = None

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

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


def _payload_error(payload: Any) -> str:
    """One-line summary of the first reason ``payload`` fails ``JsonPayload``."""
    try:
        JsonPayload.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        loc = ".".join(str(part) for part in first["loc"]) or "(root)"
        return f"{loc}: {first['msg']}"
    return "(now valid)"


def _keys_seen(payloads: list[dict], path: str | None = None) -> str:
    """Sorted union of keys across ``payloads`` (or their ``params`` sub-dicts)."""
    dicts = (
        payloads
        if path is None
        else [p[path] for p in payloads if isinstance(p.get(path), dict)]
    )
    return ", ".join(sorted({key for d in dicts for key in d})) or "(none)"


def _coverage_report(
    partition_key: str,
    routed: Counter,
    unrouted: dict[str, list[dict]],
    malformed: list[dict],
) -> str:
    """A copy-paste-actionable summary of the coverage gap.

    One block per event value that parsed but has no ``core_eel_hole_*`` table
    (or, for ``malformed``, a slug event that failed to parse at all): the count,
    the union of top-level and ``params`` keys seen across its payloads (the full
    field surface without dumping every payload), and one sample.
    """
    lines = [f"EEL-HOLE EVENT COVERAGE -- {partition_key}"]

    if unrouted:
        lines += [
            "",
            "  Parsed but NOT routed to a core_eel_hole_* table",
            "  (these events are dropped before the parquet outputs / dashboards):",
        ]
        for event_value, payloads in sorted(
            unrouted.items(), key=lambda item: -len(item[1])
        ):
            lines += [
                "",
                f"  {event_value} -- {len(payloads)} events",
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
            "  Slug events that FAILED to parse (the eel-hole log format may have",
            "  changed -- this is what fails the check):",
        ]
        for event_value, payloads in sorted(
            grouped.items(), key=lambda item: -len(item[1])
        ):
            sample = json.dumps(payloads[0], default=str, sort_keys=True)[:500]
            lines += [
                "",
                (
                    f"  {event_value} -- {len(payloads)} events -- "
                    f"{_payload_error(payloads[0])}"
                ),
                f"    sample: {sample}",
            ]

    routed_summary = ", ".join(f"{e}×{n}" for e, n in routed.most_common()) or "none"
    lines += [
        "",
        f"  Routed OK: {routed_summary}",
        "",
        "  To route a new event: add a core_eel_hole_<name> asset in",
        "  usage_metrics.core.eel_hole, a Table in usage_metrics.models, and an",
        "  Alembic migration; add it to ROUTED_EVENT_TYPES; then backfill.",
    ]
    return "\n".join(lines)


def _event_coverage_check(
    context: AssetExecutionContext, rows: list[dict], models: list[dict]
) -> AssetCheckResult:
    """Surface eel-hole events that parse but aren't routed to a persisted table.

    With the permissive models an unknown event type (or a changed payload
    shape) is no longer dropped or fatal -- it lands in ``_core_eel_hole_logs``
    and then goes nowhere, because each ``core_eel_hole_*`` table filters one
    exact ``event`` string. This makes the gap loud:

    * events parsed but not in ``ROUTED_EVENT_TYPES`` -> **WARN** (non-blocking):
      real activity we aren't persisting; add a downstream table for it.
    * slug events that failed to parse at all (bad/missing ``timestamp``, non-str
      ``event``, ...), above ``SCHEMA_DRIFT_TOLERANCE`` -> **ERROR** (blocking):
      the log *format* broke.

    The full ``_coverage_report`` is logged (WARNING / ERROR) so a maintainer
    reviewing the GHA run has the event names, field surface, and samples.
    """
    partition_key = context.partition_key
    routed: Counter = Counter()
    unrouted: dict[str, list[dict]] = {}
    malformed: list[dict] = []
    non_slug_nulled = 0

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
            continue
        if model["json_payload"] is None:
            malformed.append(payload)
        elif event in ROUTED_EVENT_TYPES:
            routed[event] += 1
        else:
            unrouted.setdefault(event, []).append(payload)

    total = (
        sum(routed.values()) + sum(len(v) for v in unrouted.values()) + len(malformed)
    )
    malformed_over_tol = len(malformed) > max(5, SCHEMA_DRIFT_TOLERANCE * total)
    unrouted_counts = Counter({e: len(v) for e, v in unrouted.items()})

    metadata = {
        "parsed_events": total - len(malformed),
        "routed_events": ", ".join(f"{e}×{n}" for e, n in routed.most_common())
        or "none",
        "unrouted_events": ", ".join(
            f"{e}×{n}" for e, n in unrouted_counts.most_common()
        )
        or "none",
        "malformed_slug_events": len(malformed),
        "non_slug_payloads_nulled": non_slug_nulled,
    }

    if malformed_over_tol:
        report = _coverage_report(partition_key, routed, unrouted, malformed)
        context.log.error(report)
        return AssetCheckResult(
            check_name=EEL_HOLE_EVENT_COVERAGE_CHECK,
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=(
                f"{partition_key}: {len(malformed)} slug eel-hole events failed to "
                "parse -- the log format may have changed. See the "
                "'EEL-HOLE EVENT COVERAGE' block in the logs."
            ),
            metadata=metadata,
        )

    if unrouted:
        report = _coverage_report(partition_key, routed, unrouted, malformed)
        context.log.warning(report)
        return AssetCheckResult(
            check_name=EEL_HOLE_EVENT_COVERAGE_CHECK,
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=(
                f"{partition_key}: parsing but NOT persisting "
                f"{metadata['unrouted_events']} -- coverage gap, add a "
                "core_eel_hole_* table (non-fatal). See the "
                "'EEL-HOLE EVENT COVERAGE' block in the logs."
            ),
            metadata=metadata,
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
    """Transform viewer.catalyst.coop logs."""
    context.log.info(f"Processing data for {context.partition_key}")

    if raw_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        yield Output(pd.DataFrame())
        yield _event_coverage_check(context, [], [])
        return

    # Flatten the many nested columns and coerce them into the expected class
    rows = raw_eel_hole_logs.to_dict(orient="records")
    models = [EelHoleLogs(**row).model_dump() for row in rows]

    converted_df = pd.json_normalize(models, sep="_")
    # Drop the columns for nested structures that json_normalize exploded (or, for
    # a partition with no parseable payloads at all, never expanded).
    converted_df = converted_df.drop(
        columns=["json_payload", "json_payload_params"], errors="ignore"
    )

    # If no record in the partition had a parseable payload, none of the
    # json_payload_* columns exist. Synthesize the (all-null) event columns the
    # rest of this transform and the downstream per-event assets select.
    if "json_payload_event" not in converted_df.columns:
        for field in JsonPayload.model_fields:
            if field not in ("timestamp", "params"):
                converted_df[f"json_payload_{field}"] = pd.NA

    # Also drop some columns that just provide constant metadata about the GCS
    # logging instance. errors="ignore": the resource / labels shape depends on
    # the deployment and GCP's logging schema, neither of which we control.
    converted_df = converted_df.drop(
        columns=[
            "log_name",
            "labels_instanceId",
            "resource_type",
            "resource_labels_configuration_name",
            "resource_labels_location",
            "resource_labels_project_id",
            "resource_labels_revision_name",
            "resource_labels_service_name",
        ],
        errors="ignore",
    )

    # JSON payload timestamp is least complete, and receive timestamp just
    # tells us when the cloud run server received the sent logs (not that interesting).
    # These vary by sub-seconds, so we'll just pick the standard 'timestamp'.
    # See https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#FIELDS.timestamp
    converted_df = converted_df.drop(
        columns=["json_payload_timestamp", "receive_timestamp"], errors="ignore"
    )

    # The filters are a list of dictionaries, so we manually split these out into
    # multiple columns for each field per query.
    # This column only exists if at least one record in the partition had search
    # filters, so we have to check before trying to process it.
    if "json_payload_params_filters" in converted_df.columns:
        # Grab only the records which are neither null nor contain an empty list in
        # this column.
        filters_df = converted_df.loc[
            (converted_df.json_payload_params_filters.notnull())
            & (converted_df.json_payload_params_filters),
            ["json_payload_params_filters"],
        ]
        filters_only = pd.DataFrame(
            filters_df["json_payload_params_filters"].to_list(), index=filters_df.index
        )

        if not filters_only.empty:
            # If we have search filters, do some maneuvering to process them
            normalized = pd.concat(
                [filters_only[i].apply(pd.Series) for i in filters_only], axis=1
            )
            normalized.columns = [
                f"json_payload_params_filters_{col.replace('.', '_')}"
                for col in pd.io.common.dedup_names(
                    normalized.columns, is_potential_multiindex=False
                )
            ]
            converted_df = converted_df.merge(
                normalized,
                how="left",
                left_index=True,
                right_index=True,
                validate="1:1",
            )

        converted_df = converted_df.drop(columns="json_payload_params_filters")

    # Remove json_payload from the column names
    converted_df.columns = converted_df.columns.str.replace("json_payload_", "")

    # Handle mixed none types in the URL column
    converted_df["url"] = converted_df["url"].replace({None: pd.NA})

    # Reformat some information about the log ins
    # A log in is made when someone hits http://viewer.catalyst.coop/callback
    converted_df.loc[
        (converted_df.event.isnull())
        & (converted_df.text_payload.str.contains("callback", na=False)),
        "event",
    ] = "log_in"

    # Whatever is after the search= gives you information about what they were searching for
    # before they logged in. We care about this!
    converted_df.loc[converted_df.event == "log_in", "log_in_query"] = (
        converted_df.loc[converted_df.event == "log_in"]
        .text_payload.map(urlsplit)
        .apply(lambda x: x.query)
        .replace(r"next=\/search(\?q%3D)?", "", regex=True)
        .str.replace("+", " ")
    )

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
    """Create table of log-in events from eel-hole logs."""
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    login_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "log_in"]
    login_df = login_df.loc[
        :, ["insert_id", "timestamp", "text_payload", "log_in_query"]
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
def core_eel_hole_hits(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of search hits from eel-hole logs."""
    context.log.info(f"Processing data for {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return pd.DataFrame()

    hit_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "hit"]
    hit_df = hit_df.loc[:, ["insert_id", "timestamp", "name", "score", "tags"]]

    return hit_df.reset_index(drop=True)


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
        + [col for col in preview_df.columns if col.startswith("params_")],
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
        + [col for col in download_df.columns if col.startswith("params_")],
    ]

    return download_df.reset_index(drop=True)


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
