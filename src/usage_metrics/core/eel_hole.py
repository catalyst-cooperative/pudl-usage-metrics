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
        "false",  # TODO: ?? I don't see this in our options, is this a weirdo that should get removed?
    ]
    value: str | int | float | None = None

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


class DuckDBParams(BaseModel):
    """DuckDB search query parameter class."""

    filters: Annotated[list[DuckDBFilters] | None, BeforeValidator(json_string_to_list)]
    name: str
    page: int
    per_page: int

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


class JsonPayload(BaseModel):
    """Portion of eel hole logs where payload is returned as a JSON."""

    event: Literal["search", "hit", "duckdb_preview", "duckdb_csv"]
    timestamp: datetime.datetime
    # Fields returned for a 'hit' response
    name: str | None = None
    query: str | None = None
    score: float | None = None
    tags: str | None = None
    # Fields returned for a 'search' or 'duckdb_preview' response
    url: Literal["/search", "/api/duckdb"] | None = None
    # Fields returned for a 'duckdb_preview' or 'duckdb_csv' response
    params: DuckDBParams | None = None

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
    def drop_non_events(cls, data):  # noqa: N805
        """Where JSON payload event is a 'loading' message, drop JSON payload."""
        if isinstance(data["jsonPayload"], dict):  # noqa: SIM102
            if (
                event := data["jsonPayload"].get("event")
            ) and "loading datapackage from" in event:
                data.pop("jsonPayload", None)
        return data

    class Config:  # noqa: D106
        alias_generator = to_camel
        populate_by_name = True


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel_hole"},
)
def core_eel_hole_logs(
    context: AssetExecutionContext,
    raw_eel_hole_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Transform viewer.catalyst.coop logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_eel_hole_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_eel_hole_logs

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
    filters_only = pd.DataFrame(filters_df["json_payload_params_filters"].to_list())
    normalized = pd.concat(
        [filters_only[i].apply(pd.Series) for i in filters_only], axis=1
    )
    normalized.columns = [
        f"json_payload_params_filters_{col.replace('.', '_')}"
        for col in pd.io.common.dedup_names(
            normalized.columns, is_potential_multiindex=False
        )
    ]
    normalized.index = (
        filters_df.index
    )  # Assign the old index to join these new columns back to their old values.
    converted_df = converted_df.merge(
        normalized, how="left", left_index=True, right_index=True, validate="1:1"
    ).drop(columns="json_payload_params_filters")

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

    # Drop any remaining rows where there is no event

    # Whatever is after the search= gives you information about what they were searching for
    # before they logged in. We care about this!
    converted_df.loc[converted_df.event == "log_in", "log_in_query"] = (
        converted_df.loc[converted_df.event == "log_in"]
        .text_payload.map(urlsplit)
        .apply(lambda x: getattr(x, "query"))
        .replace(r"next=\/search(\?q%3D)?", "", regex=True)
        .str.replace("+", " ")
    )

    # Drop some logs that are just about the app itself running.
    converted_df = converted_df.loc[
        converted_df.event.notnull(),
        :,
    ]

    context.log.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} environment.")

    return converted_df.reset_index(drop=True)
