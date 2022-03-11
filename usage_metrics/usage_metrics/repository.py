from dagster import repository, AssetGroup

from usage_metrics.assets.datasette import datasette_asset_group


@repository
def usage_metrics():
    """
    The repository definition for this usage_metrics Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    return [datasette_asset_group]
