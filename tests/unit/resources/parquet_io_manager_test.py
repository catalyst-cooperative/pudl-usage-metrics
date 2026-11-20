"""Tests for the Parquet IO manager."""

import pandas as pd
import pyarrow.parquet as pq
import pytest
from dagster import AssetKey, build_output_context

from usage_metrics.resources.parquet_io_manager import (
    PARQUET_COMPRESSION,
    PartitionedParquetIOManager,
)

TABLE = "core_github_stargazers"


@pytest.fixture
def manager(tmp_path):
    """An IO manager writing to a temporary directory."""
    return PartitionedParquetIOManager(base_path=str(tmp_path))


@pytest.fixture
def frame():
    """A small frame for ``TABLE``; the manager fills in the unspecified columns."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "starred_at": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "login": ["a", "b", "c"],
        }
    )


def test_output_is_written_with_zstd(manager, frame, tmp_path):
    """Every column chunk in the written file uses the configured codec."""
    manager.handle_output(build_output_context(asset_key=AssetKey(TABLE)), frame)
    metadata = pq.ParquetFile(tmp_path / f"{TABLE}.parquet").metadata
    codecs = {
        metadata.row_group(group).column(column).compression
        for group in range(metadata.num_row_groups)
        for column in range(metadata.num_columns)
    }
    assert codecs == {PARQUET_COMPRESSION.upper()}
