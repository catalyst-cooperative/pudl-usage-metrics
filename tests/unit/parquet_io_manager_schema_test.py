"""Test how the Parquet IO manager handles dataframes that don't match their schema.

Writing with a pyarrow schema silently drops any dataframe column the schema doesn't
list, so an upstream field that is renamed or added would just vanish. The IO manager
has to fail instead, so that the data not meeting our expectations gets fixed.
"""

import pandas as pd
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

CONTEXT_KWARGS = {
    "partition_key": "2025-01-01",
    "asset_partitions_def": DailyPartitionsDefinition(start_date="2023-08-16"),
}


# The contexts are used in `with` blocks so that the throwaway Dagster instance each
# one creates is closed right away. Left to the garbage collector, an instance can be
# closed while SQLAlchemy still has a connection to its SQLite database, which logs
# "Exception during reset or similar" errors.
def _output_context(table_name: str):
    return build_output_context(asset_key=table_name, **CONTEXT_KWARGS)


def _input_context(table_name: str):
    return build_input_context(asset_key=table_name, **CONTEXT_KWARGS)


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_extra_columns_raise_and_nothing_is_written(table_name, tmp_path) -> None:
    """A column that isn't in the schema is an error, not silently dropped."""
    manager = PartitionedParquetIOManager(base_path=str(tmp_path))
    df = pd.DataFrame(
        {name: [] for name in list(usage_metrics_schemas[table_name].columns)}
        | {"surprise_column": [], "another_surprise": []}
    )

    with _output_context(table_name) as context:
        with pytest.raises(
            ValueError, match="surprise_column.*another_surprise"
        ) as error:
            manager.handle_output(context, df)

        assert table_name in str(error.value)
        assert not manager._get_path(context).exists()


def test_renamed_column_raises_instead_of_losing_its_data(tmp_path) -> None:
    """An upstream rename looks like one missing and one extra column.

    Without the check the data under the new name is dropped and the old column
    is filled with nulls, with no sign anything went wrong.
    """
    manager = PartitionedParquetIOManager(base_path=str(tmp_path))
    df = pd.DataFrame(
        {
            "metrics_date": pd.to_datetime(["2025-01-01"]),
            "total_clones": [5],
            "uniques": [3],  # was unique_clones
        }
    )

    with (
        _output_context("core_github_clones") as context,
        pytest.raises(ValueError, match="uniques"),
    ):
        manager.handle_output(context, df)


def test_missing_columns_are_still_filled_with_nulls(tmp_path) -> None:
    """Columns can legitimately be absent from a day's data, e.g. eel hole filters.

    Those are filled with nulls rather than rejected.
    """
    manager = PartitionedParquetIOManager(base_path=str(tmp_path))
    df = pd.DataFrame(
        {"metrics_date": pd.to_datetime(["2025-01-01"]), "total_clones": [5]}
    )

    with _output_context("core_github_clones") as context:
        manager.handle_output(context, df)
    with _input_context("core_github_clones") as context:
        loaded = manager.load_input(context)

    assert loaded.total_clones.tolist() == [5]
    assert loaded.unique_clones.isna().all()


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_empty_dataframe_is_written_with_the_full_schema(table_name, tmp_path) -> None:
    """Assets return a bare pd.DataFrame() for a period with no data."""
    manager = PartitionedParquetIOManager(base_path=str(tmp_path))

    with _output_context(table_name) as context:
        manager.handle_output(context, pd.DataFrame())
    with _input_context(table_name) as context:
        loaded = manager.load_input(context)

    assert loaded.empty
    assert set(loaded.columns) == set(usage_metrics_schemas[table_name].columns)


@pytest.mark.parametrize(
    "table_name", ["core_eel_hole_previews", "core_eel_hole_downloads"]
)
def test_request_params_from_the_data_viewer_are_written(table_name, tmp_path) -> None:
    """The assets keep every ``params_*`` column, so each one needs a place in the schema.

    The viewer began logging package, table, report_date and state in September 2026,
    and database and perspective_filters on 2026-10-05, and a run failed each time
    because the IO manager refused the columns it didn't know.
    """
    manager = PartitionedParquetIOManager(base_path=str(tmp_path))
    df = pd.DataFrame(
        {
            "insert_id": ["a"],
            "params_package": ["pudl"],
            "params_table": ["core_eia861__yearly_sales"],
            "params_report_date": ["2024-01-01"],
            "params_state": ["FL"],
            "params_database": ["ferc1_dbf"],
            "params_perspective_filters": ["[]"],
        }
    )

    with _output_context(table_name) as context:
        manager.handle_output(context, df)
    with _input_context(table_name) as context:
        loaded = manager.load_input(context)

    assert loaded.params_state.tolist() == ["FL"]
    assert loaded.params_report_date.tolist() == ["2024-01-01"]
    assert loaded.params_database.tolist() == ["ferc1_dbf"]
    assert loaded.params_perspective_filters.tolist() == ["[]"]
