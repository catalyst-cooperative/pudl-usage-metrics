"""Temporary script to run scheduled dagster jobs."""
import datetime
import subprocess  # nosec B404

CONFIG_TEMPLATE = """
ops:
  extract:
    config:
      end_date: "{}"
      start_date: "{}"
"""

DATASET_JOBS = {"datasette": "process_datasette_logs_locally"}


def main():
    """Load most recent partitions of data to Google Postgres DB."""
    # Get today and yesterday's date
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    # Feed that into the template
    with open("schedule_run_config.yaml", "w") as f:
        f.write(CONFIG_TEMPLATE.format(today, yesterday))

    # Run the jobs
    for _, job in DATASET_JOBS.items():
        subprocess.run(
            [
                "dagster",
                "job",
                "execute",
                "-m",
                "usage_metrics",
                "-j",
                job,
                "-c",
                "schedule_run_config.yaml",
            ]
        )


if __name__ == "__main__":
    main()
