"""Test usage metrics dagster jobs."""
import pytest
import sqlalchemy as sa

from usage_metrics.jobs.datasette import process_datasette_logs_locally


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
