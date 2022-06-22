"""Temporary script to run scheduled dagster jobs."""
import datetime
import logging
import subprocess  # nosec B404

import coloredlogs

from usage_metrics.jobs.datasette import process_datasette_logs_gcp

CONFIG_TEMPLATE = """
ops:
  extract:
    config:
      end_date: "{}"
      start_date: "{}"
"""

DATASET_JOBS = {"datasette": process_datasette_logs_gcp}


def main():
    """Load most recent partitions of data to Google Postgres DB."""
    usage_metrics_logger = logging.getLogger("usage_metrics")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=usage_metrics_logger)

    today = datetime.date.today()

    return_codes = []

    # Run the jobs
    for _, job in DATASET_JOBS.items():
        partition_set = job.get_partition_set_def()
        most_recent_partition = max(
            partition_set.get_partitions(), key=lambda x: x.value.start
        ).value

        # Run the most recent partition if the end_date it today
        if most_recent_partition.start.date() != today:
            usage_metrics_logger.info(f"No partition for {job.name} today, skipping.")
            continue
        usage_metrics_logger.info(
            f"""Processing partition: ({most_recent_partition.start.date()},
                {most_recent_partition.end.date()}) for {job.name}."""
        )

        start_date = most_recent_partition.end.date()
        with open("schedule_run_config.yaml", "w") as f:
            f.write(CONFIG_TEMPLATE.format(today, start_date))

        return_code = subprocess.run(
            [
                "dagster",
                "job",
                "execute",
                "-m",
                "usage_metrics",
                "-j",
                job.name,
                "-c",
                "schedule_run_config.yaml",
            ]
        ).returncode
        return_codes.append(return_code)

        if return_code != 0:
            usage_metrics_logger.error(
                f"{job.name} job failed with exit code: {return_code}"
            )
        else:
            usage_metrics_logger.info(f"{job.name} job completed successfully.")

    # Exit with any failed exit codes
    if not all(x == 0 for x in return_codes):
        usage_metrics_logger.error("One of the dagster jobs failed.")
        exit(1)


if __name__ == "__main__":
    main()
