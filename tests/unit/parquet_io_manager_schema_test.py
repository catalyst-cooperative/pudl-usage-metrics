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
    LocalPartitionedParquetIOManager,
)

CONTEXT_KWARGS = {
    "partition_key": "2025-01-01",
    "asset_partitions_def": DailyPartitionsDefinition(start_date="2023-08-16"),
}


def _output_context(table_name: str):
    return build_output_context(asset_key=table_name, **CONTEXT_KWARGS)


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_extra_columns_raise_and_nothing_is_written(table_name, tmp_path) -> None:
    """A column that isn't in the schema is an error, not silently dropped."""
    manager = LocalPartitionedParquetIOManager(base_path=str(tmp_path))
    df = pd.DataFrame(
        {name: [] for name in usage_metrics_schemas[table_name].names}
        | {"surprise_column": [], "another_surprise": []}
    )
    context = _output_context(table_name)

    with pytest.raises(ValueError, match="surprise_column.*another_surprise") as error:
        manager.handle_output(context, df)

    assert table_name in str(error.value)
    assert not manager._get_path(context).exists()


def test_renamed_column_raises_instead_of_losing_its_data(tmp_path) -> None:
    """An upstream rename looks like one missing and one extra column.

    Without the check the data under the new name is dropped and the old column
    is filled with nulls, with no sign anything went wrong.
    """
    manager = LocalPartitionedParquetIOManager(base_path=str(tmp_path))
    df = pd.DataFrame(
        {
            "metrics_date": pd.to_datetime(["2025-01-01"]),
            "total_clones": [5],
            "uniques": [3],  # was unique_clones
        }
    )

    with pytest.raises(ValueError, match="uniques"):
        manager.handle_output(_output_context("core_github_clones"), df)


def test_missing_columns_are_still_filled_with_nulls(tmp_path) -> None:
    """Columns can legitimately be absent from a day's data, e.g. eel hole filters.

    Those are filled with nulls rather than rejected.
    """
    manager = LocalPartitionedParquetIOManager(base_path=str(tmp_path))
    df = pd.DataFrame(
        {"metrics_date": pd.to_datetime(["2025-01-01"]), "total_clones": [5]}
    )

    manager.handle_output(_output_context("core_github_clones"), df)
    loaded = manager.load_input(
        build_input_context(asset_key="core_github_clones", **CONTEXT_KWARGS)
    )

    assert loaded.total_clones.tolist() == [5]
    assert loaded.unique_clones.isna().all()


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_empty_dataframe_is_written_with_the_full_schema(table_name, tmp_path) -> None:
    """Assets return a bare pd.DataFrame() for a period with no data."""
    manager = LocalPartitionedParquetIOManager(base_path=str(tmp_path))

    manager.handle_output(_output_context(table_name), pd.DataFrame())
    loaded = manager.load_input(
        build_input_context(asset_key=table_name, **CONTEXT_KWARGS)
    )

    assert loaded.empty
    assert set(loaded.columns) == set(usage_metrics_schemas[table_name].names)
