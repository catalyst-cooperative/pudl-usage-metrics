"""PyTest configuration module. Defines useful fixtures, command line args."""

import logging

# Every Dagster build_*_context() call creates a throwaway instance with an in-memory
# SQLite database, and setting that up runs Alembic migrations that Alembic logs at
# INFO. pytest shows those live (log_cli_level = "INFO" in pyproject.toml), which
# drowns out the test output.
logging.getLogger("alembic").setLevel(logging.WARNING)
