"""PyTest configuration module. Defines useful fixtures, command line args."""

from pathlib import Path

import pytest

from usage_metrics.resources.sqlite import SQLiteManager


@pytest.fixture(scope="session")
def sqlite_db_path(tmpdir_factory):
    """Location of temporary sqlite database."""
    return Path(tmpdir_factory.mktemp("data")) / "usage_metrics.db"


@pytest.fixture(scope="session")
def sqlite_engine(sqlite_db_path):
    """Create a SQL Alchemy engine for sqlite fixture."""
    return SQLiteManager(db_path=sqlite_db_path).get_engine()


@pytest.fixture(scope="session")
def datasette_partition_config(sqlite_db_path):
    """Create a single partition run config for datasette logs."""
    return {
        "ops": {
            "extract": {
                "config": {"start_date": "2022-01-31", "end_date": "2022-02-06"}
            }
        },
        "resources": {
            "database_manager": {
                "config": {"db_path": str(sqlite_db_path), "clobber": False}
            }
        },
    }


@pytest.fixture(scope="session")
def intake_partition_config(sqlite_db_path):
    """Create a single partition run config for intake logs."""
    return {
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
