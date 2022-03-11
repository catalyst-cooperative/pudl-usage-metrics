from dagster import repository, AssetGroup

from usage_metrics.jobs.datasette import datasette_logs_job


@repository
def usage_metrics():
    """
    The repository definition for this usage_metrics Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    return [datasette_logs_job]
