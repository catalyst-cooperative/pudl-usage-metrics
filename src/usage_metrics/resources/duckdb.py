"""Dagster SQLite IOManager."""

from pathlib import Path

from dagster import io_manager
from dagster_duckdb_pandas import DuckDBPandasIOManager

from usage_metrics.models import usage_metrics_metadata

DUCKDB_PATH = Path(__file__).parents[3] / "data/usage_metrics.duckdb"


@io_manager()
def duckdb_pandas_io_manager(init_context) -> DuckDBPandasIOManager:
    """Create a DuckDB IO manager for Pandas dataframes."""
    return DuckDBPandasIOManager(
        schema=usage_metrics_metadata, database=str(DUCKDB_PATH)
    )
