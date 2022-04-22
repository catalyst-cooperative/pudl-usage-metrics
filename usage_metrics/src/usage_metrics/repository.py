"""Usage metrics dagster repository."""
from dagster import repository

from usage_metrics.jobs.datasette import process_datasette_logs


@repository
def usage_metrics():
    """Define dagster repository for usage_metrics."""
    return [process_datasette_logs]
