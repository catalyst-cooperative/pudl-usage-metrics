"""Test usage metrics dagster jobs."""

import pandas as pd
import pytest
from usage_metrics.jobs.intake import process_intake_logs_locally


@pytest.mark.xfail(reason="Xfail until we reconfigure datasette ETL.")
def test_intake_job(sqlite_engine, intake_partition_config):
    """Process a single partition of intake logs."""
    result = process_intake_logs_locally.execute_in_process(
        run_config=intake_partition_config
    )

    assert result.success

    # Make sure we got the correct number of rows.
    with sqlite_engine.connect() as con:
        logs = pd.read_sql(
            "select insert_id from intake_logs",
            con,
        )
    assert len(logs) == 6
