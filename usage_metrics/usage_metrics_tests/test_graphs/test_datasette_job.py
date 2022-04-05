"""Test usage metrics dagster jobs."""
from usage_metrics.jobs.datasette import datasette_logs_job


def test_datasette_job():
    """This is an example test for a Dagster job."""
    result = datasette_logs_job.execute_in_process()

    assert result.success
