"""Test dagster graphs."""
from usage_metrics.jobs.say_hello import say_hello_job


def test_say_hello():
    """This is an example test for a Dagster job."""
    result = say_hello_job.execute_in_process()

    assert result.success
    assert result.output_for_node("hello") == "Hello, Dagster!"
