"""PyTest configuration module. Defines useful fixtures, command line args."""

import logging

import pandera.pyarrow as pandera
import pytest

from usage_metrics.models import _column, _table_schema

# Every Dagster build_*_context() call creates a throwaway instance with an in-memory
# SQLite database, and setting that up runs Alembic migrations that Alembic logs at
# INFO. pytest shows those live (log_cli_level = "INFO" in pyproject.toml), which
# drowns out the test output.
logging.getLogger("alembic").setLevel(logging.WARNING)


@pytest.fixture
def toy_schema() -> pandera.DataFrameSchema:
    """A small table schema with a primary key and a column with a pattern."""
    return _table_schema(
        "toy",
        [
            _column("id", str, "ID."),
            _column("tls", str, "TLS version.", pattern=r"-|TLSv1\.[0-3]"),
        ],
        "A toy table.",
        primary_key=["id"],
    )
