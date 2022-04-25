"""PyTest configuration module. Defines useful fixtures, command line args."""
import pytest


@pytest.fixture(scope="session")
def sqlite_db_path(tmpdir_factory):
    """Location of temporary sqlite database."""
    return tmpdir_factory.mktemp("data") / "usage_metrics.db"
