"""Usage metrics dagster repository."""
import importlib
import pkgutil
from pathlib import Path

from dagster import JobDefinition, repository


@repository
def usage_metrics():
    """Define dagster repository for usage_metrics."""
    jobs = []

    jobs_subpackage_path = Path(__file__).parent / "jobs"
    modules = [str(jobs_subpackage_path)]

    for module_info in pkgutil.iter_modules(modules):
        module = importlib.import_module(f"usage_metrics.jobs.{module_info.name}")

        # Get all of the JobDefinitions
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if type(attr) == JobDefinition:
                jobs.append(attr)

    return jobs
