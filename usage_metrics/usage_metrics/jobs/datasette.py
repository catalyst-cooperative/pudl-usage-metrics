"""Datasette ELT dagster job."""
from dagster import in_process_executor

from usage_metrics.assets.datasette import datasette_asset_group

datasette_logs_job = datasette_asset_group.build_job(
    name="datasette_logs", executor_def=in_process_executor
)
