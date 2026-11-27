"""Tests of data safety against real GCS, which the fakes can't vouch for.

They use the ``live_gcs_path`` fixture, so they only run when asked to.
"""

import pandas as pd
import pytest
from dagster import DailyPartitionsDefinition, Failure, build_output_context

from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager


def test_empty_results_never_replace_rows_in_gcs(live_gcs_path):
    """The guard relies on how pyarrow reports missing and existing GCS objects."""
    manager = PartitionedParquetIOManager(base_path=live_gcs_path)

    def write(rows: int) -> None:
        frame = pd.DataFrame(
            {"metrics_date": pd.to_datetime(["2026-01-01"] * rows), "total_clones": 1}
        )
        with build_output_context(
            asset_key="core_github_clones",
            partition_key="2026-01-01",
            asset_partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
        ) as context:
            manager.handle_output(context, frame)

    write(0)  # Nothing there yet
    write(0)  # An empty file may be replaced
    write(1)  # And filled
    with pytest.raises(Failure):
        write(0)
