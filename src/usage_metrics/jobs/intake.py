"""Intake logs ETL dagster jobs."""
from dagster import graph, in_process_executor

from usage_metrics.ops.intake import extract, load, transform
from usage_metrics.resources.sqlite import sqlite_manager


@graph
def process_intake_logs():
    """Process extract, clean and load logs to a destination."""
    raw_logs = extract()
    transformed_logs = transform(raw_logs)
    load(transformed_logs)


process_intake_logs_locally = process_intake_logs.to_job(
    # config=intake_daily_partition,
    resource_defs={"database_manager": sqlite_manager},
    executor_def=in_process_executor,
    name="process_intake_logs_locally",
)

# process_datasette_logs_gcp = process_datasette_logs.to_job(
#     config=datasette_daily_partition,
#     resource_defs={"database_manager": postgres_manager},
#     executor_def=in_process_executor,
#     name="process_datasette_logs_gcp",
# )
