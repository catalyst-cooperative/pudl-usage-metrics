"""Run the most recent partition for every job in the gcp_usage_metrics dagster repository.

This script runs daily in the load-metrics Github Action.

Note: Eventually this script should be deprecated in
favor of having a long running dagster instance handle
schedules and job launching.
"""

import logging
import os

import coloredlogs

from usage_metrics.etl import defs


def main():
    """Load most recent partitions of data to Google Cloud SQL Postgres DB."""
    usage_metrics_logger = logging.getLogger("usage_metrics")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=usage_metrics_logger)

    usage_metrics_logger.info(
        f"""Saving to {os.getenv("METRICS_PROD_ENV", "local")} database."""
    )
    # Run the partitioned metrics
    job = defs.get_job_def(name="all_partitioned_metrics_etl")

    # Get last complete weekly partition
    most_recent_partition = max(job.partitions_def.get_partition_keys())

    # Run the jobs
    usage_metrics_logger.info(
        f"""{job.name}: Processing partitioned data from the week of {most_recent_partition}."""
    )

    job.execute_in_process(partition_key=most_recent_partition)

    # Run the non-partitioned metrics
    usage_metrics_logger.info(
        f"""{job.name}: Processing the most recent non-partitioned data."""
    )
    job = defs.get_job_def(name="all_nonpartitioned_metrics_etl")
    job.execute_in_process()


if __name__ == "__main__":
    main()
