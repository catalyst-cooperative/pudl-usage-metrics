from dagster import repository

from usage_metrics.jobs.say_hello import say_hello_job
from usage_metrics.schedules.my_hourly_schedule import my_hourly_schedule
from usage_metrics.sensors.my_sensor import my_sensor


@repository
def usage_metrics():
    """
    The repository definition for this usage_metrics Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
