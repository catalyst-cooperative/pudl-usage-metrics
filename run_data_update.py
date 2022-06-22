"""Temporary script to run scheduled dagster jobs."""
import datetime
import logging

import coloredlogs

from usage_metrics.jobs.datasette import process_datasette_logs_gcp

DATASET_JOBS = {"datasette": process_datasette_logs_gcp}


def main():
    """Load most recent partitions of data to Google Postgres DB."""
    usage_metrics_logger = logging.getLogger("usage_metrics")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=usage_metrics_logger)

    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    # Run the jobs
    for _, job in DATASET_JOBS.items():
        partition_set = job.get_partition_set_def()
        most_recent_partition = max(
            partition_set.get_partitions(), key=lambda x: x.value.start
        )
        time_window = most_recent_partition.value

        # Run the most recent partition if the end_date it yesterday
        if time_window.start.date() != yesterday:
            usage_metrics_logger.info(
                f"No partition for {job.name} yesterday, skipping."
            )
            continue
        usage_metrics_logger.info(
            f"""Processing partition: ({time_window.start.date()},
                {time_window.end.date()}) for {job.name}."""
        )

        job.execute_in_process(partition_key=most_recent_partition.name)


if __name__ == "__main__":
    main()
