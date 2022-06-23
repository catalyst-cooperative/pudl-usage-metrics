"""Temporary script to run scheduled dagster jobs."""
import datetime
import logging

import coloredlogs

from usage_metrics.repository import gcp_usage_metrics


def main():
    """Load most recent partitions of data to Google Postgres DB."""
    usage_metrics_logger = logging.getLogger("usage_metrics")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=usage_metrics_logger)

    today = datetime.date.today()

    # Run the jobs
    for job in gcp_usage_metrics.get_all_jobs():
        partition_set = job.get_partition_set_def()
        most_recent_partition = max(
            partition_set.get_partitions(), key=lambda x: x.value.start
        )
        time_window = most_recent_partition.value

        # Run the most recent partition if the end_date is today
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
