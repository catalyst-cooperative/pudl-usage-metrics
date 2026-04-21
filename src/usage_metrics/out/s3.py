"""Create outputs from S3 logs."""

import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    asset,
)

REQUESTERS_IGNORE = [
    "arn:aws:iam::638819805183:user/intake.catalyst.coop-admin",
    "arn:aws:iam::638819805183:user/pudl-s3-logs-sync",
    "arn:aws:sts::652627389412:assumed-role/roda-checker-ScheduledFunctionRole-1PKVG6H08EE8I/roda-checker-ScheduledFunction-MWYE7Y123CDJ",
]


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    # io_manager_key="parquet_manager",
    # kinds={"parquet"},
    tags={"source": "s3"},
)
def out_s3_logs(
    context: AssetExecutionContext,
    core_s3_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Output daily S3 logs.

    Filter to GET requests, drop Catalyst and AWS traffic, and add version/table
    columns. Also drop old EPACEMS files, JSON files, and traffic where no
    data is sent.
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if core_s3_logs.empty:
        return core_s3_logs
    # Only keep GET requests
    out = core_s3_logs.loc[
        (core_s3_logs.operation == "REST.GET.BUCKET")
        | (core_s3_logs.operation == "REST.GET.OBJECT")
    ]

    # Only keep records with data sent
    out = out.loc[out.megabytes_sent > 0]

    # Drop PUDL intake, AWS Registry of Open Data Checker, and PUDL logs sync
    out = out.loc[~out.requester.isin(REQUESTERS_IGNORE)]

    # Add columns for tables and versions
    out[["version", "table"]] = out["key"].str.split("/", expand=True, n=1)
    out["version"] = out["version"].replace(["-", ""], pd.NA)

    # Drop .js files and old accidental hourly emissions data
    out = out.loc[
        ~(
            out.table.str.endswith(".js")
            | out.table.str.startswith("hourly_emissions_epacems/")
        )
    ]

    # Drop logfiles
    out = out[~out.table.str.endswith((".log", ".csv", "env"))]  # Clean up cruft

    # Treat different zips as same format
    out = out.replace(["sqlite.gz", "sqlite.zip"], ["sqlite", "sqlite"], regex=True)

    out.loc[out.table.str.contains("hourly_emissions"), "table"] = (
        "core_epacems__hourly_emissions.parquet"  # We renamed this table
    )

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
        # Before this date, lump usage into one category
        out["usage_type"] = "all"

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


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    # io_manager_key="parquet_manager",
    # kinds={"parquet"},
    tags={"source": "s3"},
)
def out_s3_daily_summary_by_table(
    out_s3_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Get a daily summary by table from out_s3_logs."""
    summary_df = (
        pl.DataFrame(out_s3_logs)
        .filter(pl.col("table").str.ends_with(".parquet"))
        .sort(["time", "table"])
        .group_by_dynamic("time", every="1d", group_by=["usage_type", "table"])
        .agg(
            normalized_file_downloads=pl.col("normalized_file_downloads")
            .sum()
            .round(0),
            request_count=pl.col("request_uri").count().alias("request_count"),
            megabytes_sent=(pl.col("megabytes_sent").sum().round(3)),
            unique_ips=pl.col("remote_ip").n_unique(),
        )
    )
    return summary_df.to_pandas()
