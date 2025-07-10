"""Transform data from viewer.catalyst.coop logs."""

import json
import os
from urllib.parse import urlsplit

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)

from usage_metrics.helpers import convert_camel_case_columns_to_snake_case


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "eel-hole"},
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

    # Flatten the many nested columns
    json_df = pd.json_normalize(raw_eel_hole_logs.jsonPayload).rename(
        columns={"timestamp": "json_timestamp"}
    )  # Rename to avoid duplicate column names
    labels_df = pd.json_normalize(raw_eel_hole_logs.labels)
    df = raw_eel_hole_logs.drop(
        columns=["jsonPayload", "labels", "resource"]
    )  # We just don't care about 'resource', which tracks some cloud logging stuff we'd prefer to monitor in GCS directly

    df = pd.concat([df, json_df, labels_df], axis=1)

    # Convert column names to snake case
    df = convert_camel_case_columns_to_snake_case(df)
    df.columns = df.columns.str.replace(".", "_")

    # Deduplicate timestamps and round down
    df = df.drop(columns=["timestamp", "receive_timestamp"]).rename(
        columns={"json_timestamp": "timestamp"}
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Handle params filter column, which is a string of a list of JSON dictionaries
    df.loc[df.params_filters.notnull(), "params_filters"] = df.loc[
        df.params_filters.notnull()
    ].params_filters.apply(json.loads)
    filters = pd.DataFrame(
        df.loc[df.params_filters.notnull(), "params_filters"].tolist()
    )
    filters.columns = [f"filter_{n}" for n in filters.columns]
    filters_df = pd.concat(
        [filters[i].apply(pd.Series).add_prefix(f"{i}_") for i in filters.iloc[:, :]],
        axis=1,
    ).pipe(convert_camel_case_columns_to_snake_case)
    expanded_filters_df = (
        df.loc[df.params_filters.notnull()].reset_index().join(filters_df)
    )
    df = (
        pd.concat(
            [expanded_filters_df.set_index("index"), df.loc[df.params_filters.isnull()]]
        )
        .reset_index(drop=True)
        .drop(columns="params_filters")
    )

    # Drop some logs that are just about the app itself running.
    df = df.loc[
        ~df.text_payload.str.contains(
            "Serving Flask app|Debug mode: off", regex=True, na=False
        ),
        :,
    ]

    # Scrap some information about the log ins
    # A log in is made when someone hits http://viewer.catalyst.coop/callback
    df.loc[
        (df.event.isnull()) & (df.text_payload.str.contains("callback")), "event"
    ] = "log_in"

    # Whatever is after the search= gives you information about what they were searching for
    # before they logged in. We care about this!
    df.loc[df.event == "log_in", "log_in_query"] = (
        df.loc[df.event == "log_in"]
        .text_payload.map(urlsplit)
        .apply(lambda x: getattr(x, "query"))
        .replace(r"next=\/search(\?q%3D)?", "", regex=True)
        .str.replace("+", " ")
    )

    context.log.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} environment.")

    return df.reset_index()
