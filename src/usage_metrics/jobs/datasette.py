"""Datasette ELT dagster job."""
from datetime import datetime

import pandas as pd
from dagster import daily_partitioned_config, graph, in_process_executor

import usage_metrics.ops.datasette as da
from usage_metrics.resources.postgres import postgres_manager
from usage_metrics.resources.sqlite import sqlite_manager


@daily_partitioned_config(start_date=datetime(2022, 1, 24))
def datasette_daily_partition(start: datetime, end: datetime):
    """Dagster daily partition config for datasette logs."""
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


@graph
def transform(raw_logs: pd.DataFrame) -> pd.DataFrame:
    """Transform datasette logs."""
    df = da.unpack_httprequests(raw_logs)
    df = da.parse_urls(df)
    return da.geocode_ips(df)


@graph
def process_datasette_logs():
    """Process datasette logs locally using a SQLite database."""
    raw_logs = da.extract()
    transformed_logs = transform(raw_logs)
    da.load(transformed_logs)


process_datasette_logs_locally = process_datasette_logs.to_job(
    config=datasette_daily_partition,
    resource_defs={"database_manager": sqlite_manager},
    executor_def=in_process_executor,
    name="process_datasette_logs_locally",
)

process_datasette_logs_gcp = process_datasette_logs.to_job(
    config=datasette_daily_partition,
    resource_defs={"database_manager": postgres_manager},
    executor_def=in_process_executor,
    name="process_datasette_logs_gcp",
)
