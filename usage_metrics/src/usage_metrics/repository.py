"""Usage metrics dagster repository."""
from dagster import repository

from usage_metrics.jobs.datasette import process_datasette_logs_locally


@repository
def usage_metrics():
    """Define dagster repository for usage_metrics."""
    return [process_datasette_logs_locally]
