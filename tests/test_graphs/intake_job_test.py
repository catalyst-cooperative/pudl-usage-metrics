"""Test usage metrics dagster jobs."""
import pandas as pd

from usage_metrics.jobs.intake import process_intake_logs_locally
from usage_metrics.resources.sqlite import SQLiteManager


def test_intake_job(sqlite_db_path):
    """Process a single partition of intake logs."""
    result = process_intake_logs_locally.execute_in_process(
        run_config={
            "ops": {
                "extract": {
                    "config": {"start_date": "2022-07-12", "end_date": "2022-07-13"}
                }
            },
            "resources": {
                "database_manager": {
                    "config": {"db_path": str(sqlite_db_path), "clobber": True}
                }
            },
        }
    )

    assert result.success

    # Make sure we got the correct number of rows.
    engine = SQLiteManager(db_path=sqlite_db_path).get_engine()
    with engine.connect() as con:
        logs = pd.read_sql(
            "select insert_id from intake_logs",
            con,
        )
    assert len(logs) == 6
