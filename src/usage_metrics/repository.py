"""
Create repositories for each datasource.

Dagster repositories are a means of organizing jobs. The repositories
are organized by datasource.
"""

from dagster import repository

from usage_metrics.jobs.datasette import (
    process_datasette_logs_gcp,
    process_datasette_logs_locally,
)
from usage_metrics.jobs.intake import process_intake_logs_locally


@repository
def datasette_logs():
    """Define dagster repository of jobs that populate datasette logs."""
    return [process_datasette_logs_locally, process_datasette_logs_gcp]


@repository
def intake_logs():
    """Define dagster repository of jobs that process Intake logs."""
    return [process_intake_logs_locally]
