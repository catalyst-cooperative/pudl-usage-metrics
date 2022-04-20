"""Datasette ELT dagster job."""
from datetime import datetime

from dagster import job, weekly_partitioned_config

from usage_metrics.ops.datasette import (
    clean_datasette_logs,
    data_request_logs,
    raw_logs,
    unpack_httprequests,
)
from usage_metrics.resources.sqlite import sqlite_manager


@weekly_partitioned_config(start_date=datetime(2022, 1, 31))
def datasette_weekly_partition(start: datetime, end: datetime):
    """Dagster weekly partition config for datasette logs."""
    return {
        "ops": {
            "raw_logs": {
                "config": {
                    "start_date": start.strftime("%Y-%m-%d"),
                    "end_date": end.strftime("%Y-%m-%d"),
                }
            }
        }
    }


@job(
    config=datasette_weekly_partition, resource_defs={"sqlite_manager": sqlite_manager}
)
def process_datasette_logs():
    """Process datasette logs."""
    df = raw_logs()
    df = unpack_httprequests(df)
    df = clean_datasette_logs(df)
    data_request_logs(df)
