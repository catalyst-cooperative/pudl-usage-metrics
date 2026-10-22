"""Fixtures for writing tables through the Parquet IO manager and checking them."""

from collections.abc import Callable
from contextlib import AbstractContextManager

import pandas as pd
import pandera.pyarrow as pandera
import pyarrow.types as pat
import pytest
from dagster import (
    DailyPartitionsDefinition,
    OutputContext,
    PythonObjectDagsterType,
    build_input_context,
    build_output_context,
)

from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager
from usage_metrics.schemas import arrow_schema

PARTITION_KEY = "2025-01-01"
CONTEXT_KWARGS = {
    "partition_key": PARTITION_KEY,
    "asset_partitions_def": DailyPartitionsDefinition(start_date="2023-08-16"),
}

# Two different values that match the pattern of each column that has one in its schema.
PATTERNED_VALUES = {
    "bucket_owner": ["a" * 64, "b" * 64],
    "remote_ip": ["192.0.2.3", "2001:db8::1"],
    "operation": ["REST.GET.OBJECT", "REST.HEAD.OBJECT"],
    "request_uri": ["GET /a HTTP/1.1", "-"],
    "signature_version": ["SigV4", "SigV2"],
    "tls_version": ["TLSv1.3", "-"],
}


@pytest.fixture
def parquet_manager(tmp_path) -> PartitionedParquetIOManager:
    """A Parquet IO manager writing under a temporary directory."""
    return PartitionedParquetIOManager(base_path=str(tmp_path))


# The contexts are used in `with` blocks so that the throwaway Dagster instance each
# one creates is closed right away. Left to the garbage collector, an instance can be
# closed while SQLAlchemy still has a connection to its SQLite database, which logs
# "Exception during reset or similar" errors.
@pytest.fixture
def output_context() -> Callable[[str], AbstractContextManager[OutputContext]]:
    """Build an output context for one partition of a table, to use in a `with`."""
    return lambda table_name: build_output_context(
        asset_key=table_name, **CONTEXT_KWARGS
    )


@pytest.fixture
def write_then_load(
    parquet_manager: PartitionedParquetIOManager,
) -> Callable[[str, pd.DataFrame], pd.DataFrame]:
    """Write a dataframe through the IO manager and read it back."""

    def _write_then_load(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        with build_output_context(
            asset_key=table_name, **CONTEXT_KWARGS
        ) as output_context:
            parquet_manager.handle_output(output_context, df)
        with build_input_context(
            asset_key=table_name,
            dagster_type=PythonObjectDagsterType(object),
            **CONTEXT_KWARGS,
        ) as input_context:
            return parquet_manager.load_input(input_context)

    return _write_then_load


@pytest.fixture
def distinct_values() -> Callable[[pandera.DataFrameSchema], dict[str, list]]:
    """Make two rows of values that differ per column, so a column mix-up shows up."""

    def _distinct_values(schema: pandera.DataFrameSchema) -> dict[str, list]:
        patterned = {name for name, column in schema.columns.items() if column.checks}
        missing = patterned - set(PATTERNED_VALUES)
        assert not missing, f"Add values matching the patterns of {sorted(missing)}."
        values = {}
        for i, field in enumerate(arrow_schema(schema)):
            if pat.is_boolean(field.type):
                column = [i % 2 == 0, i % 2 == 1]
            elif pat.is_integer(field.type):
                column = [i, i + 1000]
            elif pat.is_floating(field.type):
                column = [i + 0.25, i + 0.75]
            elif pat.is_timestamp(field.type):
                column = [
                    pd.Timestamp("2025-01-01") + pd.Timedelta(days=i),
                    pd.Timestamp("2025-06-01") + pd.Timedelta(days=i),
                ]
            else:
                column = PATTERNED_VALUES.get(
                    field.name, [f"{field.name}-a", f"{field.name}-b"]
                )
            values[field.name] = column
        # The IO manager overwrites partition_key with the Dagster partition key.
        if "partition_key" in values:
            values["partition_key"] = [PARTITION_KEY] * 2
        return values

    return _distinct_values
