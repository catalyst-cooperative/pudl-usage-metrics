"""
Run the most recent partition for every job in the gcp_usage_metrics dagster repository.

This script runs daily in the load-metrics Github Action.

Note: Eventually this script should be deprecated in
favor of having a long running dagster instance handle
schedules and job launching.
"""

import logging
from datetime import datetime, timezone

import coloredlogs
from dagster import RepositoryDefinition

from usage_metrics import repository
from usage_metrics.resources.postgres import postgres_manager


def main():
    """Load most recent partitions of data to Google Cloud SQL Postgres DB."""
    usage_metrics_logger = logging.getLogger("usage_metrics")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=usage_metrics_logger)

    today = datetime.now(tz=timezone.utc).date()

    # Collect GCP jobs
    gcp_jobs = []

    for attr_name in dir(repository):
        attr = getattr(repository, attr_name)
        if type(attr) == RepositoryDefinition:
            for job in attr.get_all_jobs():
                if job.resource_defs["database_manager"] == postgres_manager:
                    gcp_jobs.append(job)

    # Run the jobs
    for job in gcp_jobs:
        partition_set = job.get_partition_set_def()
        most_recent_partition = max(
            partition_set.get_partitions(), key=lambda x: x.value.start
        )
        time_window = most_recent_partition.value
        usage_metrics_logger.info(time_window)

        # Raise an error if the time window is less than a day
        time_window_diff = (time_window.end - time_window.start).in_days()
        if time_window_diff != 1:
            raise RuntimeError(
                f"""The {job.name} job's partition is less than a day.
                                    Choose a less frequent partition definition."""
            )

        # Run the most recent partition if the end_date is today.
        # The start_date is inclusive and the end_date is exclusive.
        if time_window.end.date() == today:
            usage_metrics_logger.info(
                f"""Processing partition: ({time_window.start.date()},
                {time_window.end.date()}) for {job.name}."""
            )

            job.execute_in_process(partition_key=most_recent_partition.name)
        else:
            usage_metrics_logger.info(
                f"No scheduled partition for {job.name} yesterday, skipping."
            )


if __name__ == "__main__":
    main()
