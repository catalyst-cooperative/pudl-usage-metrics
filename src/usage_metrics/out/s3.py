"""Create outputs from S3 logs."""

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)

REQUESTERS_IGNORE = [
    "arn:aws:iam::638819805183:user/intake.catalyst.coop-admin",
    "arn:aws:iam::638819805183:user/pudl-s3-logs-sync",
    "arn:aws:sts::652627389412:assumed-role/roda-checker-ScheduledFunctionRole-1PKVG6H08EE8I/roda-checker-ScheduledFunction-MWYE7Y123CDJ",
]


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "s3"},
)
def out_s3_logs(
    context: AssetExecutionContext,
    core_s3_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Output daily S3 logs.

    Filter to GET requests, drop Catalyst and AWS traffic, and add version/table
    columns.
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if core_s3_logs.empty:
        return core_s3_logs
    # Only keep GET requests
    out = core_s3_logs.loc[
        (core_s3_logs.operation == "REST.GET.BUCKET")
        | (core_s3_logs.operation == "REST.GET.OBJECT")
    ]

    # Drop PUDL intake, AWS Registry of Open Data Checker, and PUDL logs sync
    out = out.loc[~out.requester.isin(REQUESTERS_IGNORE)]

    # Add columns for tables and versions
    out[["version", "table"]] = out["key"].str.split("/", expand=True, n=1)
    out["version"] = out["version"].replace(["-", ""], pd.NA)

    # Categorize usage types
    # Only after the eel-hole bucket was created on Nov 24th.
    if pd.to_datetime(context.partition_key) >= pd.to_datetime("2025-11-24"):
        # First, default to "other_s3" - these are direct S3 queries.
        out["usage_type"] = "other_s3"
        # If the data has a referer type of data.catalyst.coop, it is definitely coming
        # from the eel-hole, but it's not definitively a link click or a DuckDB search.
        # We label all data.catalyst.coop referers as eel_hole_link, then overwrite
        # All data types where the data is being queried from the eel hole bucket as
        # eel_hole_duckdb as these are definitively preview/download queries.
        out["usage_type"] = out["usage_type"].mask(
            out.referer.str.startswith("https://data.catalyst.coop/"), "eel_hole_link"
        )
        out["usage_type"] = out["usage_type"].mask(
            out.version == "eel-hole", "eel_hole_duckdb"
        )
        # Finally, we label all queries originating from our docs.
        out["usage_type"] = out["usage_type"].mask(
            out.referer.str.contains(
                "pudl.readthedocs.io|docs.catalyst.coop", regex=True
            ),
            "docs_link",
        )
    else:
        out["usage_type"] = pd.NA

    # Drop columns
    out = out.drop(
        columns=[
            "bucket_owner",
            "requester",
            "operation",
            "bucket",
        ]
    )

    return out
