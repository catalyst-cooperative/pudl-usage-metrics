"""Test that nothing in the Parquet IO manager depends on column order.

Column order is not meaningful in this project: columns should always be looked up
by name. These tests write each table with its dataframe columns and/or its schema
fields in a different order and require that, compared by name, nothing changes.
"""

import pandas as pd
import pandera.pyarrow as pandera
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dagster import (
    DailyPartitionsDefinition,
    build_input_context,
    build_output_context,
)

from usage_metrics.models import usage_metrics_schemas
from usage_metrics.resources.parquet_io_manager import (
    PartitionedParquetIOManager,
)
from usage_metrics.schemas import arrow_schema

PARTITION_KEY = "2025-01-01"
PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2023-08-16")


# Two different values for each string column that has a pattern in its schema.
PATTERNED_VALUES = {
    "bucket_owner": ["a" * 64, "b" * 64],
    "remote_ip": ["192.0.2.3", "2001:db8::1"],
    "operation": ["REST.GET.OBJECT", "REST.HEAD.OBJECT"],
    "request_uri": ["GET /a HTTP/1.1", "-"],
    "signature_version": ["SigV4", "SigV2"],
    "tls_version": ["TLSv1.3", "-"],
}


def _distinct_values(schema: pa.Schema) -> dict[str, list]:
    """Make two rows of values that differ per column, so a column mix-up shows up."""
    values = {}
    for i, field in enumerate(schema):
        if pa.types.is_boolean(field.type):
            column = [i % 2 == 0, i % 2 == 1]
        elif pa.types.is_integer(field.type):
            column = [i, i + 1000]
        elif pa.types.is_floating(field.type):
            column = [i + 0.25, i + 0.75]
        elif pa.types.is_timestamp(field.type):
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


def _write_then_load(
    manager: PartitionedParquetIOManager, table_name: str, df: pd.DataFrame
) -> pd.DataFrame:
    kwargs = {
        "asset_key": table_name,
        "partition_key": PARTITION_KEY,
        "asset_partitions_def": PARTITIONS_DEF,
    }
    # Close the contexts explicitly; see parquet_io_manager_schema_test.py.
    with build_output_context(**kwargs) as output_context:
        manager.handle_output(output_context, df)
    with build_input_context(**kwargs) as input_context:
        return manager.load_input(input_context)


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
@pytest.mark.parametrize(
    "reverse_dataframe,reverse_schema",
    [(True, False), (False, True), (True, True)],
    ids=["reversed_dataframe", "reversed_schema", "reversed_both"],
)
def test_parquet_round_trip_independent_of_column_order(
    table_name: str,
    reverse_dataframe: bool,
    reverse_schema: bool,
    tmp_path,
    monkeypatch,
) -> None:
    """Every value comes back in the column with its name, whatever the order."""
    schema = usage_metrics_schemas[table_name]
    expected = pd.DataFrame(_distinct_values(arrow_schema(schema)))

    df = expected.copy()
    if reverse_dataframe:
        df = df[list(reversed(df.columns))]
    if reverse_schema:
        monkeypatch.setitem(
            usage_metrics_schemas,
            table_name,
            pandera.DataFrameSchema(
                dict(reversed(schema.columns.items())),
                unique=schema.unique,
                strict=False,
                description=schema.description,
            ),
        )

    manager = PartitionedParquetIOManager(base_path=str(tmp_path))
    loaded = _write_then_load(manager, table_name, df)

    pd.testing.assert_frame_equal(loaded, expected, check_like=True, check_dtype=False)


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_written_parquet_passes_its_pandera_check(table_name: str, tmp_path) -> None:
    """What the IO manager writes is what the schema asset check will validate."""
    schema = usage_metrics_schemas[table_name]
    df = pd.DataFrame(_distinct_values(arrow_schema(schema)))
    manager = PartitionedParquetIOManager(base_path=str(tmp_path))

    with build_output_context(
        asset_key=table_name,
        partition_key=PARTITION_KEY,
        asset_partitions_def=PARTITIONS_DEF,
    ) as context:
        manager.handle_output(context, df)
        written = pq.read_table(str(manager._get_path(context)))

    schema.validate(written, lazy=True)
