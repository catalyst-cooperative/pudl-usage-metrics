"""Transform data from viewer.catalyst.coop logs."""

import datetime
import json
import math
import os
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)
from pydantic import BaseModel, BeforeValidator, field_validator, model_validator
from pydantic.alias_generators import to_camel


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

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


class DuckDBParams(BaseModel):
    """DuckDB search query parameter class."""

    filters: Annotated[
        list[DuckDBFilters] | None, BeforeValidator(json_string_to_list)
    ]  # Convert JSON string to list before validating
    name: str
    page: int
    per_page: int

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


class JsonPayload(BaseModel):
    """Portion of eel hole logs where payload is returned as a JSON."""

    event: Literal["search", "hit", "duckdb_preview", "duckdb_csv", "privacy-policy"]
    timestamp: datetime.datetime
    user_id: str | None = None
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

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True

    @field_validator("*", mode="before")
    def replace_na_with_none(cls, value, info):  # noqa: N805
        """To successfully validate string dtypes, convert NaNs to None."""
        if info.field_name == "score":
            return value  # Skip NaN coercion for the 'score' field
        if isinstance(value, float) and math.isnan(value):
            return None
        return value


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

    @model_validator(mode="before")
    def drop_bad_records(cls, data):  # noqa: N805
        """Where JSON payload event is a 'loading' message or params badly formatted, drop JSON payload."""
        if isinstance(data["jsonPayload"], dict):  # noqa: SIM102
            if (
                # If event is a "loading" message
                (
                    (event := data["jsonPayload"].get("event"))
                    and "loading datapackage from" in event
                )
                # Or if params are malformed
                or (
                    (params := data["jsonPayload"].get("params"))
                    and (params.get("name") is not None)
                    and (params.get("page") is None)
                )
            ):
                data.pop("jsonPayload", None)
        return data

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "eel_hole"},
)
def _core_eel_hole_logs(
    context: AssetExecutionContext,
    raw_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Transform viewer.catalyst.coop logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return pd.DataFrame()

    # Flatten the many nested columns and coerce them into the expected class
    # Then drop the two columns (json_payload and json_payload_params) that we exploded
    # into many other columns and are now empty
    models = [
        EelHoleLogs(**row).model_dump()
        for row in raw_eel_hole_logs.to_dict(orient="records")
    ]
    converted_df = pd.json_normalize(models, sep="_").drop(
        columns=["json_payload", "json_payload_params"]
    )

    # Also drop some columns that just provide constant metadata about the GCS logging
    # instance
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
            "resource_labels_service_name",
        ]
    )

    # JSON payload timestamp is least complete, and receive timestamp just
    # tells us when the cloud run server received the sent logs (not that interesting).
    # These vary by sub-seconds, so we'll just pick the standard 'timestamp'.
    # See https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#FIELDS.timestamp
    converted_df = converted_df.drop(
        columns=["json_payload_timestamp", "receive_timestamp"]
    )

    # The filters are a list of dictionaries, so we manually split these out into
    # multiple columns for each field per query.
    # Grab only the records which are neither null nor contain an empty list in this column.
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
            normalized, how="left", left_index=True, right_index=True, validate="1:1"
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
        & (converted_df.text_payload.str.contains("callback")),
        "event",
    ] = "log_in"

    # Whatever is after the search= gives you information about what they were searching for
    # before they logged in. We care about this!
    converted_df.loc[converted_df.event == "log_in", "log_in_query"] = (
        converted_df.loc[converted_df.event == "log_in"]
        .text_payload.map(urlsplit)
        .apply(lambda x: getattr(x, "query"))
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

    return converted_df.reset_index(drop=True)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel_hole"},
)
def core_eel_hole_log_ins(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of log-in events from eel-hole logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return pd.DataFrame()

    login_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "log_in"]
    login_df = login_df.loc[
        :, ["insert_id", "timestamp", "text_payload", "log_in_query"]
    ]

    return login_df.reset_index(drop=True)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel_hole"},
)
def core_eel_hole_searches(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of search events from eel-hole logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return pd.DataFrame()

    search_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "search"]
    search_df = search_df.loc[
        :, ["insert_id", "user_id", "timestamp", "query", "url", "session_id"]
    ]

    return search_df.reset_index(drop=True)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel_hole"},
)
def core_eel_hole_hits(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of search hits from eel-hole logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return pd.DataFrame()

    hit_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "hit"]
    hit_df = hit_df.loc[:, ["insert_id", "timestamp", "name", "score", "tags"]]

    return hit_df.reset_index(drop=True)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel_hole"},
)
def core_eel_hole_previews(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of DuckDB preview requests from eel-hole logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return pd.DataFrame()

    preview_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "duckdb_preview"]
    preview_df = preview_df.loc[
        :,
        ["insert_id", "user_id", "timestamp", "url", "session_id"]
        + [col for col in preview_df.columns if col.startswith("params_")],
    ]

    return preview_df.reset_index(drop=True)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel_hole"},
)
def core_eel_hole_downloads(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of DuckDB download requests from eel-hole logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return pd.DataFrame()

    download_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "duckdb_csv"]
    download_df = download_df.loc[
        :,
        ["insert_id", "user_id", "timestamp", "url", "session_id"]
        + [col for col in download_df.columns if col.startswith("params_")],
    ]

    return download_df.reset_index(drop=True)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel_hole"},
)
def core_eel_hole_user_settings_updates(
    context: AssetExecutionContext,
    _core_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Create table of user setting updates."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if _core_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return pd.DataFrame()

    settings_df = _core_eel_hole_logs[_core_eel_hole_logs.event == "privacy-policy"]
    settings_df = settings_df.loc[
        :,
        ["insert_id", "user_id", "timestamp", "accepted", "newsletter", "outreach"],
    ]

    # If user_id is none, there shouldn't really be a log here.
    # Drop these weirdo records.
    settings_df = settings_df.loc[settings_df.user_id.notnull()]

    return settings_df.reset_index(drop=True)
