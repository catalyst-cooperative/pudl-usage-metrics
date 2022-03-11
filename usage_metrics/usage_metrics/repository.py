from dagster import repository

from usage_metrics.jobs.datasette import process_datasette_logs


@repository
def usage_metrics():
    """
    The repository definition for this usage_metrics Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [process_datasette_logs]
    
    return jobs
