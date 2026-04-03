"""Run the most recent partition for every job in the gcp_usage_metrics dagster repository.

This script runs daily in the load-metrics Github Action.

Note: Eventually this script should be deprecated in
favor of having a long running dagster instance handle
schedules and job launching.
"""

import logging
import os

import click
import coloredlogs

from usage_metrics.etl import defs


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option("-p", "--partition", type=str, default=None)
def main(partition: str | None):
    """Load most recent partitions of data to Google Cloud Storage."""
    usage_metrics_logger = logging.getLogger("usage_metrics")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=usage_metrics_logger)

    usage_metrics_logger.info(
        f"""Saving to {os.getenv("METRICS_PROD_ENV", "local")} storage."""
    )
    # Run the partitioned metrics
    job = defs.get_job_def(name="all_partitioned_metrics_etl")

    # Get last complete weekly partition
    if partition:
        assert partition in job.partitions_def.get_partition_keys(), (
            f"{partition} isn't a valid partition. Valid partitions are: {job.partitions_def.get_partition_keys()}"
        )
    else:
        partition = max(job.partitions_def.get_partition_keys())

    # Run the jobs
    usage_metrics_logger.info(
        f"""{job.name}: Processing partitioned data from the week of {partition}."""
    )

    job.execute_in_process(partition_key=partition)

    # Run the non-partitioned metrics
    usage_metrics_logger.info(
        f"""{job.name}: Processing the most recent non-partitioned data."""
    )
    job = defs.get_job_def(name="all_nonpartitioned_metrics_etl")
    job.execute_in_process()


if __name__ == "__main__":
    main()
