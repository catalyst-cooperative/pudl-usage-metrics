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

    # Only keep GET requests
    out = core_s3_logs.loc[
        (core_s3_logs.operation == "REST.GET.BUCKET")
        | (core_s3_logs.operation == "REST.GET.OBJECT")
    ]

    # Drop PUDL intake, AWS Registry of Open Data Checker, and PUDL logs sync
    out = out.loc[~out.requester.isin(REQUESTERS_IGNORE)]

    # Add columns for tables and versions
    out[["version", "table"]] = out["key"].str.split("/", expand=True)
    out["version"] = out["version"].replace(["-", ""], pd.NA)

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
