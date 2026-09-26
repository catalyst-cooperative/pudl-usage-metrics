"""Dagster parquet IO manager.

Adapted from example at
https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py
"""

from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetCheckExecutionContext,
    ConfigurableIOManager,
    ConfigurableResource,
    InputContext,
    OutputContext,
)
from upath import UPath

from usage_metrics.helpers import get_table_name_from_context
from usage_metrics.models import usage_metrics_schemas

ARROW_TO_PANDAS: dict[pa.DataType, str] = {
    pa.bool_(): "bool",
    pa.int64(): "Int64",
    pa.float64(): "float64",
    pa.string(): "string",
    pa.timestamp("s"): "datetime64[s]",
}
"""Type map so we can derive pandas dtypes from the pyarrow schema."""


def _parquet_path(
    base_path: str, table_name: str, partition_window: tuple[datetime, datetime] | None
) -> UPath:
    """Compute the parquet path for a table, partitioned or not."""
    if partition_window is not None:
        start, end = partition_window
        dt_format = "%Y-%m-%d"
        partition_str = start.strftime(dt_format) + "--" + end.strftime(dt_format)
        return UPath(base_path) / table_name / f"{partition_str}.parquet"
    return UPath(base_path) / f"{table_name}.parquet"


class PartitionedParquetIOManager(ConfigurableIOManager):
    """An IOManager that writes and retrieves data frames from parquet files.

    It stores partitioned outputs nested under the primary asset key.

    `base_path` may be a local directory or a remote URI (e.g. `gs://bucket`)
    -- UPath handles both transparently, so a single class covers local and
    remote storage.
    """

    base_path: str

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Save a data frame to a parquet file."""
        path = self._get_path(context)
        if "://" not in self.base_path:
            path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(obj, pd.DataFrame):
            row_count = len(obj)
            context.log.debug(f"Row count: {row_count}")
            table_name = get_table_name_from_context(context)
            assert (
                table_name in usage_metrics_schemas
            ), f"""{table_name} does not have a schema defined.
                Create a schema for it in usage_metrics.models."""
            schema = usage_metrics_schemas[table_name]
            table_dtypes = {f.name: ARROW_TO_PANDAS[f.type] for f in schema}
            # Make sure we have all the columns we need
            for column, dtype in table_dtypes.items():
                if column not in obj.columns:
                    obj[column] = pd.Series(dtype=dtype)
            # If a table has data, and is supposed to have a partition key,
            # create a partition_key column to enable subsetting a partition
            # when reading out of Parquet.
            if not obj.empty and "partition_key" in table_dtypes:
                assert context.has_partition_key, (
                    f"Expected partition key for table {table_name} but none found in context"
                )
                obj["partition_key"] = context.partition_key
            # delocalize datetimes
            obj = obj.assign(
                **{
                    c: pd.to_datetime(obj[c]).dt.tz_localize(None)
                    for c in obj.columns
                    if c in table_dtypes and table_dtypes[c] == "datetime64[s]"
                }
            )
            # we need the .astype because int nulls in string-object columns make Arrow sad
            # we need the str() because passing in a remote UPath with gs protocol confuses Pandas
            obj.astype(table_dtypes).to_parquet(
                path=str(path),
                index=False,
                schema=schema,
            )
        else:
            raise TypeError(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": str(path)})

    def load_input(self, context) -> pd.DataFrame | str:
        """Load a data frame from a parquet file."""
        path = self._get_path(context)
        return pd.read_parquet(str(path))

    def _get_path(self, context: InputContext | OutputContext) -> UPath:
        """Compute the parquet path for this asset."""
        key = context.asset_key.path[-1]
        window = (
            context.asset_partitions_time_window
            if context.has_asset_partitions
            else None
        )
        return _parquet_path(self.base_path, key, window)


class PyArrowTableReader(ConfigurableResource):
    """Reads parquet outputs directly as pyarrow Tables, bypassing pandas.

    `base_path` may be a local directory or a remote URI (e.g. `gs://bucket`)
    -- UPath handles both transparently, so a single class covers local and
    remote storage.

    This is a plain reader, not an IOManager: Dagster doesn't allow an asset
    check to load its own target asset through a second IOManager via
    `additional_ins` (the same asset key can't be passed to both `asset=` and
    `additional_ins=`), so schema-validating asset checks call `read_table`
    directly instead. It's used by pandera asset checks that validate data
    with pandera's pyarrow backend, so validation runs on the same Arrow data
    written by PartitionedParquetIOManager with no pandas round-trip.
    """

    base_path: str

    def read_table(
        self, table_name: str, context: AssetCheckExecutionContext
    ) -> pa.Table:
        """Read a table's parquet file(s) as a pyarrow Table."""
        window = context.partition_time_window if context.has_partition_key else None
        path = _parquet_path(self.base_path, table_name, window)
        return pq.read_table(str(path))
