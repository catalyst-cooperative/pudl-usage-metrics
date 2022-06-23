"""Usage metrics dagster repository."""
import importlib
import pkgutil
from pathlib import Path

from dagster import JobDefinition, repository

from usage_metrics.resources.postgres import postgres_manager
from usage_metrics.resources.sqlite import sqlite_manager

gcp_jobs = []
local_jobs = []

jobs_subpackage_path = Path(__file__).parent / "jobs"
modules = [str(jobs_subpackage_path)]

for module_info in pkgutil.iter_modules(modules):
    module = importlib.import_module(f"usage_metrics.jobs.{module_info.name}")

    # Get all of the JobDefinitions
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if type(attr) == JobDefinition:
            if attr.resource_defs["database_manager"] == sqlite_manager:
                local_jobs.append(attr)
            elif attr.resource_defs["database_manager"] == postgres_manager:
                gcp_jobs.append(attr)
            else:
                raise RuntimeError(
                    f"{attr_name} does not have a valid database manager resource."
                )


@repository
def gcp_usage_metrics():
    """Define dagster repository of jobs that populate local sqlite db."""
    return gcp_jobs


@repository
def local_usage_metrics():
    """Define dagster repository of jobs that populate Cloud SQL DB."""
    return local_jobs
