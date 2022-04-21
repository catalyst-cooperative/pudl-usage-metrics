"""Datasette ELT dagster job."""
from datetime import datetime

import pandas as pd
from dagster import graph, in_process_executor, job, weekly_partitioned_config

import usage_metrics.ops.datasette as da
from usage_metrics.resources.sqlite import sqlite_manager


@graph
def transform(raw_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Datasette log transformation graph.

    Args:
        raw_logs: The raw logs extracted from BQ.

    Return:
        df: The cleaned logs.
    """
    df = da.unpack_httprequests(raw_logs)
    df = da.parse_urls(df)
    df = da.geocode_ips(df)
    return df


@weekly_partitioned_config(start_date=datetime(2022, 1, 24))
def datasette_weekly_partition(start: datetime, end: datetime):
    """Dagster weekly partition config for datasette logs."""
    return {
        "ops": {
            "extract": {
                "config": {
                    "start_date": start.strftime("%Y-%m-%d"),
                    "end_date": end.strftime("%Y-%m-%d"),
                }
            }
        }
    }


@job(
    config=datasette_weekly_partition,
    resource_defs={"database_manager": sqlite_manager},
    executor_def=in_process_executor,
)
def process_datasette_logs():
    """Process datasette logs."""
    df = da.extract()
    df = transform(df)
    df = da.load(df)
