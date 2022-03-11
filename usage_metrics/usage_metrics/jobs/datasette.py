from dagster import job, weekly_partitioned_config, io_manager
from datetime import datetime

from usage_metrics.ops.datasette import extract, unpack_httprequests
from usage_metrics.resources.postgres import DataframePostgresIOManager


@io_manager
def df_to_postgres_io_manager(_):
    return DataframePostgresIOManager()

@job(resource_defs={"io_manager": df_to_postgres_io_manager})
def process_datasette_logs():
    """
    A job definition. This example job has a single op.

    For more hints on writing Dagster jobs, see our documentation overview on Jobs:
    https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs
    """
    raw_logs = extract()
    unpack_httprequests(raw_logs)
