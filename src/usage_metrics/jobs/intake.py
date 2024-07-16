"""Intake logs ETL dagster jobs."""

from datetime import datetime

from dagster import daily_partitioned_config, graph, in_process_executor

from usage_metrics.ops.intake import extract, load, transform
from usage_metrics.resources.postgres import postgres_manager
from usage_metrics.resources.sqlite import sqlite_manager


@daily_partitioned_config(start_date=datetime(2022, 5, 9))
def intake_daily_partition(start: datetime, end: datetime):
    """Dagster daily partition config for intake logs."""
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
def process_intake_logs():
    """Process extract, clean and load logs to a destination."""
    raw_logs = extract()
    transformed_logs = transform(raw_logs)
    load(transformed_logs)


process_intake_logs_locally = process_intake_logs.to_job(
    config=intake_daily_partition,
    resource_defs={"database_manager": sqlite_manager},
    executor_def=in_process_executor,
    name="process_intake_logs_locally",
)

process_intake_logs_gcp = process_intake_logs.to_job(
    config=intake_daily_partition,
    resource_defs={"database_manager": postgres_manager},
    executor_def=in_process_executor,
    name="process_intake_logs_gcp",
)
