"""
Create repositories for GCP and local jobs.

Dagster JobDefinitions are collected from each module in
the usage_metrics.jobs subpackage. Each module in the usage_metrics.jobs
subpackage should have a postgres and sqlite job for gcp and local
processing respectively.

Dagster repositories are a means of organizing jobs. The repositories
are separated by the destination so it is easy to run all jobs
for a given destination.
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
