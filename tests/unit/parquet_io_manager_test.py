"""Test the fidelity of what the Parquet IO manager writes and reads back.

Column order is not meaningful in this project: columns should always be looked up
by name. The first tests write each table with its dataframe columns and/or its schema
fields in a different order and require that, compared by name, nothing changes.

The next test requires that every table is stored with exactly the Arrow types of its
schema, passes its pandera check, and comes back as pandas with exactly the dtypes the
schema declares, with its nulls where they were and its values unchanged.
"""

import pandas as pd
import pandera.pyarrow as pandera
import pyarrow.parquet as pq
import pytest
from dagster import Failure
from upath import UPath

from usage_metrics.models import usage_metrics_schemas
from usage_metrics.resources.parquet_io_manager import (
    PartitionedParquetIOManager,
)
from usage_metrics.schemas import arrow_schema, pandas_dtypes

# Column order is a property of the IO manager, not of a table, so a few tables will do:
# core_s3_logs has every column type, and the others have a primary key made of one
# column or of several.
ORDER_TABLES = ["core_s3_logs", "core_github_clones", "core_zenodo_logs"]


@pytest.mark.parametrize("table_name", ORDER_TABLES)
@pytest.mark.parametrize(
    "reverse_dataframe,reverse_schema",
    [(True, False), (False, True), (True, True)],
    ids=["reversed_dataframe", "reversed_schema", "reversed_both"],
)
def test_parquet_round_trip_independent_of_column_order(
    table_name: str,
    reverse_dataframe: bool,
    reverse_schema: bool,
    write_then_load,
    distinct_values,
    monkeypatch,
) -> None:
    """Every value comes back in the column with its name, whatever the order."""
    schema = usage_metrics_schemas[table_name]
    expected = pd.DataFrame(distinct_values(schema)).astype(pandas_dtypes(schema))

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

    loaded = write_then_load(table_name, df)

    pd.testing.assert_frame_equal(loaded, expected, check_like=True)


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_table_is_stored_and_read_back_with_the_types_of_its_schema(
    table_name: str, tmp_path, write_then_load, distinct_values
) -> None:
    """A table survives a round trip through the IO manager unchanged.

    The Parquet file has exactly the Arrow types of the table's schema and passes its
    pandera check. Read back as pandas, the table has exactly the dtypes of the schema,
    with its nulls where they were and its values unchanged.
    """
    schema = usage_metrics_schemas[table_name]
    # Two rows. Key columns get distinct values, columns that can't be null get the
    # same non-null value twice, and every other column is null in the second row.
    key = set(schema.unique or [])
    required = {name for name, column in schema.columns.items() if not column.nullable}
    df = pd.DataFrame(
        {
            name: column
            if name in key
            else [column[0], column[0] if name in required else None]
            for name, column in distinct_values(schema).items()
        }
    )

    loaded = write_then_load(table_name, df)

    (path,) = (tmp_path / table_name).glob("*.parquet")
    stored = pq.read_schema(str(path))
    declared = arrow_schema(schema)
    assert {f.name: f.type for f in stored} == {f.name: f.type for f in declared}
    schema.validate(pq.read_table(str(path)), lazy=True)
    assert {c: str(t) for c, t in loaded.dtypes.items()} == pandas_dtypes(schema)
    # The IO manager fills in partition_key itself, so it isn't one of the inputs.
    for column in df.columns.drop("partition_key", errors="ignore"):
        assert loaded[column].isna().tolist() == df[column].isna().tolist(), column
        assert loaded[column].dropna().tolist() == df[column].dropna().tolist(), column


def _clones(n_rows: int) -> pd.DataFrame:
    """``n_rows`` of clone counts, or the bare empty frame assets return for no data."""
    if n_rows == 0:
        return pd.DataFrame()
    return pd.DataFrame(
        {
            "metrics_date": pd.date_range("2025-01-01", periods=n_rows),
            "total_clones": range(n_rows),
        }
    )


def test_an_empty_result_never_replaces_rows(
    write_then_load, parquet_manager, output_context
) -> None:
    """Missing input or a bug can't erase data we may not be able to produce again."""
    table_name = "core_github_clones"
    write_then_load(table_name, _clones(1))

    with output_context(table_name) as context:
        with pytest.raises(Failure):
            parquet_manager.handle_output(context, _clones(0))

        stored = pq.read_table(str(parquet_manager._get_path(context)))

    assert stored.num_rows == 1


@pytest.mark.parametrize(
    ("before", "after"),
    [(0, 0), (0, 2), (1, 2), (2, 1)],
    ids=["still_empty", "gap_filled", "more_rows", "fewer_rows"],
)
def test_other_results_replace_the_file(write_then_load, before, after) -> None:
    """Only emptying a file is refused, so reruns can fill gaps and correct data."""
    write_then_load("core_github_clones", _clones(before))

    assert len(write_then_load("core_github_clones", _clones(after))) == after


def test_base_path_accepts_a_path_and_keeps_a_remote_uri_intact(tmp_path) -> None:
    """A Path or UPath is stored as a string; a remote one keeps its scheme.

    The IO manager treats a base path with "://" in it as remote and doesn't try to
    create directories for it.
    """
    assert PartitionedParquetIOManager(base_path=tmp_path).base_path == str(tmp_path)
    remote = PartitionedParquetIOManager(
        base_path=UPath("gs://bucket/prefix")  # type: ignore[bad-argument-type]
    )
    assert remote.base_path == "gs://bucket/prefix"
