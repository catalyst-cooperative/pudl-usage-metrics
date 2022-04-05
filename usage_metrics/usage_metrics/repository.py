"""Usage metrics dagster repository."""
from dagster import repository

from usage_metrics.jobs.datasette import datasette_logs_job


@repository
def usage_metrics():
    """Define dagster repository for usage_metrics."""
    return [datasette_logs_job]
