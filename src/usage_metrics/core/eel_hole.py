"""Transform data from viewer.catalyst.coop logs."""

import datetime
import json
import math
import os
import re
from collections import Counter
from collections.abc import Iterator
from typing import Annotated, Any, Literal, get_args
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

ALLOWABLE_EVENT_TYPES = Literal[
    "search", "hit", "duckdb_preview", "duckdb_csv", "privacy-policy"
]

EEL_HOLE_SCHEMA_DRIFT_CHECK = "eel_hole_schema_drift"

IGNORED_EVENT_TYPES: frozenset[str] = frozenset({"loading"})
"""``jsonPayload.event`` values that are known viewer noise, not real user
events, and shouldn't count toward schema drift. ``loading`` is a client-side
"still loading" marker. Add benign event types here (rather than loosening the
tolerance) as they turn up."""

SCHEMA_DRIFT_TOLERANCE = 0.01
"""Fraction of parsed events that may be event-bearing-but-unparseable before
``_schema_drift_check`` fails the partition (with a floor of 5, so tiny days
don't trip on a single bad line). Above this, assume the viewer's log schema
changed rather than sporadic bad log lines."""


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


class DuckDBFilters(BaseModel):
    """DuckDB filter format class."""

    field_name: str
    field_type: Literal["text", "number", "date"]
    operation: Literal[
        "equals",
        "contains",
        "greaterThan",
        "greaterThanOrEqual",
        "lessThan",
        "lessThanOrEqual",
        "notBlank",
        "startsWith",
        "notEqual",
        "notContains",
        "inRange",
        "blank",
        "false",
        "true",
    ]
    value: str | int | float | None = None

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class DuckDBParams(BaseModel):
    """DuckDB search query parameter class."""

    filters: Annotated[
        list[DuckDBFilters] | None, BeforeValidator(json_string_to_list)
    ]  # Convert JSON string to list before validating
    name: str
    page: int
    per_page: int

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class JsonPayload(BaseModel):
    """Portion of eel hole logs where payload is returned as a JSON."""

    event: ALLOWABLE_EVENT_TYPES
    timestamp: datetime.datetime
    user_id: str | None = None
    user_domain: str | None = None
    # Fields returned for a 'hit' response
    name: str | None = None
    query: str | None = None
    score: float | None = None
    tags: str | None = None
    # Fields returned for a 'search' or 'duckdb_preview' response
    url: str | None = None
    # Fields returned for a 'duckdb_preview' or 'duckdb_csv' response
    params: DuckDBParams | None = None
    # Fields returned for a 'privacy-policy' event
    # We don't persist these as they are logged in the user
    # database, but why not validate them anyways?
    accepted: bool | None = None
    newsletter: bool | None = None
    outreach: bool | None = None

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    @field_validator("*", mode="before")
    def replace_na_with_none(cls, value, info):  # noqa: N805
        """To successfully validate string dtypes, convert NaNs to None."""
        if info.field_name == "score":
            return value  # Skip NaN coercion for the 'score' field
        if isinstance(value, float) and math.isnan(value):
            return None
        return value


def payload_is_parseable(value: Any) -> bool:
    """Whether a raw ``jsonPayload`` value can be parsed as a ``JsonPayload`` event.

    The single source of truth for which eel-hole log lines carry usable event
    data. Loading messages and other non-event lines, payloads missing a valid
    ``event`` or ``timestamp``, non-dict payloads, and malformed ``params``
    objects (empty, partial, bad filter shapes, unknown filter operations) all
    return ``False``; their payload is then nulled so the row falls out
    downstream where event-less rows are filtered.
    """
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
    def null_unparseable_payload(cls, value):  # noqa: N805
        """Null a JSON payload that isn't a parseable event.

        Nulling it here -- rather than letting ``EelHoleLogs`` validation raise
        and kill the whole partition -- lets the row fall out downstream. See
        ``payload_is_parseable`` for exactly what counts as parseable.
        """
        if isinstance(value, JsonPayload) or payload_is_parseable(value):
            return value
        return None

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


_EVENT_SLUG = re.compile(r"[a-z][a-z0-9_-]{0,40}\Z")


def _payload_error(payload: Any) -> str:
    """One-line summary of the first reason ``payload`` fails ``JsonPayload``."""
    try:
        JsonPayload.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        loc = ".".join(str(part) for part in first["loc"]) or "(root)"
        return f"{loc}: {first['msg']}"
    return "(now valid)"


def _drift_report(
    partition_key: str, total_event_bearing: int, dropped: list[dict]
) -> str:
    """A copy-paste-actionable summary of the dropped event-bearing payloads."""
    allowable = set(get_args(ALLOWABLE_EVENT_TYPES))
    counts = Counter(str(payload["event"]) for payload in dropped)
    samples: dict[str, dict] = {}
    for payload in dropped:
        samples.setdefault(str(payload["event"]), payload)

    lines = [
        f"EEL-HOLE SCHEMA DRIFT -- {partition_key}",
        (
            f"  {len(dropped)} of {total_event_bearing} event-bearing payloads "
            f"({len(dropped) / total_event_bearing:.0%}) failed to parse "
            f"(tolerance {SCHEMA_DRIFT_TOLERANCE:.0%})."
        ),
        "",
        f"  {'dropped event value':<44}{'count':>7}  problem",
        f"  {'-' * 44}{'-' * 7}  {'-' * 45}",
    ]
    for event_value, count in counts.most_common():
        if event_value in allowable:
            problem = _payload_error(samples[event_value])
        elif _EVENT_SLUG.match(event_value):
            problem = "not in ALLOWABLE_EVENT_TYPES (new or renamed event?)"
        else:
            problem = "not a slug -- likely a log message landing in `event`"
        lines.append(f"  {event_value[:44]:<44}{count:>7}  {problem}")

    lines.append("")
    lines.append("  sample payloads:")
    for event_value, payload in samples.items():
        dumped = json.dumps(payload, default=str, sort_keys=True)
        lines.append(f"    {event_value[:44]}: {dumped[:500]}")

    lines += [
        "",
        "  to fix, in usage_metrics.core.eel_hole:",
        "    - new/renamed real event -> add to ALLOWABLE_EVENT_TYPES, model it",
        "    - changed field or filter on a known event -> update the model",
        "    - benign log noise -> add the exact event value to IGNORED_EVENT_TYPES",
        f"  then reprocess partition {partition_key}.",
    ]
    return "\n".join(lines)


def _schema_drift_check(
    context: AssetExecutionContext, rows: list[dict], models: list[dict]
) -> AssetCheckResult:
    """Fail the partition when log lines that look like events no longer parse.

    ``null_unparseable_payload`` silently nulls any payload it can't parse, which
    is right for the app-noise log lines but would also quietly discard real
    events if the viewer changed its log schema (a new/renamed event type, a
    changed field, a new filter operation). This re-inspects the payloads that
    carried an ``event`` key (other than ``IGNORED_EVENT_TYPES``) but were
    dropped, and fails when they exceed ``SCHEMA_DRIFT_TOLERANCE`` of the
    event-bearing payloads -- a systematic break (forgot to update parsing,
    upstream schema change) shows up as a large fraction; a few is treated as
    sporadic bad lines. On failure it logs a full ``_drift_report`` at ERROR so
    the fix is obvious from the GHA logs without further digging.
    """
    partition_key = context.partition_key
    parsed_events = sum(model["json_payload"] is not None for model in models)
    dropped = [
        row["jsonPayload"]
        for row, model in zip(rows, models, strict=True)
        if isinstance(row.get("jsonPayload"), dict)
        and "event" in row["jsonPayload"]
        and str(row["jsonPayload"]["event"]) not in IGNORED_EVENT_TYPES
        and model["json_payload"] is None
    ]
    total_event_bearing = parsed_events + len(dropped)
    over_tolerance = len(dropped) > max(5, SCHEMA_DRIFT_TOLERANCE * total_event_bearing)
    counts = Counter(str(payload["event"]) for payload in dropped)
    unrecognized = sorted(set(counts) - set(get_args(ALLOWABLE_EVENT_TYPES)))

    if dropped:
        report = _drift_report(partition_key, total_event_bearing, dropped)
        (context.log.error if over_tolerance else context.log.warning)(report)

    if over_tolerance:
        top = ", ".join(f"{value}×{n}" for value, n in counts.most_common(8))
        description = (
            f"{partition_key}: {len(dropped)}/{total_event_bearing} "
            f"({len(dropped) / total_event_bearing:.0%}) event-bearing eel-hole "
            f"payloads failed to parse (tolerance {SCHEMA_DRIFT_TOLERANCE:.0%}). "
            f"Dropped: {top}. See the 'EEL-HOLE SCHEMA DRIFT' block in the logs, "
            "fix usage_metrics.core.eel_hole, then reprocess."
        )
    else:
        description = (
            f"{partition_key}: eel-hole schema drift within tolerance "
            f"({len(dropped)} dropped)."
        )

    return AssetCheckResult(
        check_name=EEL_HOLE_SCHEMA_DRIFT_CHECK,
        passed=not over_tolerance,
        severity=AssetCheckSeverity.ERROR,
        description=description,
        metadata={
            "event_bearing_payloads": total_event_bearing,
            "dropped": len(dropped),
            "dropped_fraction": (
                round(len(dropped) / total_event_bearing, 4)
                if total_event_bearing
                else 0.0
            ),
            "dropped_event_values": (
                ", ".join(f"{value}×{n}" for value, n in counts.most_common()) or "none"
            ),
            "unrecognized_event_types": ", ".join(unrecognized) or "none",
        },
    )


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "eel_hole"},
    check_specs=[
        AssetCheckSpec(
            name=EEL_HOLE_SCHEMA_DRIFT_CHECK,
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
        yield _schema_drift_check(context, [], [])
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
    yield _schema_drift_check(context, rows, models)


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
