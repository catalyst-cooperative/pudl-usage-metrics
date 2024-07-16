"""Unit tests for dagster resources."""

import pandas as pd
import pytest
from usage_metrics.resources.sqlite import SQLiteManager


def test_missing_schema() -> None:
    """Test missing schema assertion."""
    sq = SQLiteManager()
    with pytest.raises(AssertionError):
        sq.append_df_to_table(pd.DataFrame(), "fake_name")
