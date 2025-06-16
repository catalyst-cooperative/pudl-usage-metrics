"""Unit tests for dagster resources."""

from pathlib import Path

import pandas as pd
import pytest
from dagster import build_output_context

from usage_metrics.resources.sqlite import SQLiteIOManager


def test_missing_schema(sqlite_db_path) -> None:
    """Test missing schema assertion."""
    sq = SQLiteIOManager(db_path=Path(sqlite_db_path))
    context = build_output_context(partition_key="1980-01-01")
    with pytest.raises(AssertionError):
        sq.append_df_to_table(context, pd.DataFrame(), "fake_name")
