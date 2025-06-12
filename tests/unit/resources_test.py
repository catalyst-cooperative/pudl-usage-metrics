"""Unit tests for dagster resources."""

import pandas as pd
import pytest
from dagster import build_output_context

from usage_metrics.resources.sqlite import sqlite_manager


def test_missing_schema() -> None:
    """Test missing schema assertion."""
    sq = sqlite_manager()
    context = build_output_context(partition_key="1980-01-01")
    with pytest.raises(AssertionError):
        sq.append_df_to_table(context, pd.DataFrame(), "fake_name")
