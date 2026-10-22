"""Test how the Parquet IO manager handles dataframes that don't match their schema.

Writing with a pyarrow schema silently drops any dataframe column the schema doesn't
list, so an upstream field that is renamed or added would just vanish. The IO manager
has to fail instead, so that the data not meeting our expectations gets fixed.
"""

import pandas as pd
import pytest

from usage_metrics.models import usage_metrics_schemas


def test_extra_columns_raise_and_nothing_is_written(
    parquet_manager, output_context
) -> None:
    """A column that isn't in the schema is an error, not silently dropped.

    This is what an upstream rename looks like: a column missing and a new one, whose
    data would otherwise be dropped while the old column is filled with nulls.
    """
    table_name = "core_github_clones"
    df = pd.DataFrame(
        {name: [] for name in list(usage_metrics_schemas[table_name].columns)}
        | {"surprise_column": [], "another_surprise": []}
    )

    with output_context(table_name) as context:
        with pytest.raises(
            ValueError, match="surprise_column.*another_surprise"
        ) as error:
            parquet_manager.handle_output(context, df)

        assert table_name in str(error.value)
        assert not parquet_manager._get_path(context).exists()


def test_missing_columns_are_still_filled_with_nulls(write_then_load) -> None:
    """Columns can legitimately be absent from a day's data, e.g. eel hole filters.

    Those are filled with nulls rather than rejected.
    """
    df = pd.DataFrame(
        {"metrics_date": pd.to_datetime(["2025-01-01"]), "total_clones": [5]}
    )

    loaded = write_then_load("core_github_clones", df)

    assert loaded.total_clones.tolist() == [5]
    assert loaded.unique_clones.isna().all()


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_empty_dataframe_is_written_with_the_full_schema(
    table_name, write_then_load
) -> None:
    """Assets return a bare pd.DataFrame() for a period with no data."""

    loaded = write_then_load(table_name, pd.DataFrame())

    assert loaded.empty
    assert set(loaded.columns) == set(usage_metrics_schemas[table_name].columns)
