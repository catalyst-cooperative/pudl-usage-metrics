"""Test usage metrics dagster jobs."""
import pandas as pd
import pytest
import sqlalchemy as sa

from usage_metrics.jobs.datasette import process_datasette_logs_locally
from usage_metrics.resources.sqlite import SQLiteManager


def test_datasette_job(sqlite_db_path):
    """Process a single partition  of datassette."""
    result = process_datasette_logs_locally.execute_in_process(
        run_config={
            "ops": {
                "extract": {
                    "config": {"start_date": "2022-01-31", "end_date": "2022-02-06"}
                }
            },
            "resources": {
                "database_manager": {"config": {"db_path": str(sqlite_db_path)}}
            },
        }
    )

    assert result.success

    # Make sure we got the correct number of rows.
    engine = SQLiteManager(db_path=sqlite_db_path).get_engine()
    with engine.connect() as con:
        logs = pd.read_sql(
            "select insert_id from datasette_request_logs"
            " where timestamp < '2022-02-06'",
            con,
        )
    assert len(logs) == 891


def test_primary_key_failure(sqlite_db_path):
    """Reprocess the same partition as `test_datasette_job` test for integrity error."""
    with pytest.raises(sa.exc.IntegrityError):
        _ = process_datasette_logs_locally.execute_in_process(
            run_config={
                "ops": {
                    "extract": {
                        "config": {"start_date": "2022-01-31", "end_date": "2022-02-06"}
                    }
                },
                "resources": {
                    "database_manager": {"config": {"db_path": str(sqlite_db_path)}}
                },
            }
        )
