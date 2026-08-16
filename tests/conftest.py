"""PyTest configuration module. Defines useful fixtures, command line args."""

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def sqlite_db_path(tmpdir_factory):
    """Location of temporary sqlite database."""
    return Path(tmpdir_factory.mktemp("data")) / "usage_metrics.db"
