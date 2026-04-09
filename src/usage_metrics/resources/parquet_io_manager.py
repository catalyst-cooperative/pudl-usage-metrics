"""Dagster parquet IO manager.

Adapted from example at
https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py
"""

import os

import pandas as pd
import pyarrow as pa
from dagster import (
    ConfigurableIOManager,
    Field,
    InputContext,
    OutputContext,
    io_manager,
)
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, Integer, String
from upath import UPath

from usage_metrics.helpers import get_table_name_from_context
from usage_metrics.models import usage_metrics_metadata

PANDAS_TO_ARROW: dict[str, pa.DataType] = {
    "bool": pa.bool_(),
    "date": pa.date32(),  # not currently used but maybe someday
    "datetime64[s]": pa.timestamp("s"),
    "Int32": pa.int32(),
    "Int64": pa.int64(),
    "float64": pa.float64(),
    "string": pa.string(),
}
"""Type map so we can annotate empty partitions."""

SQLALCHEMY_TO_PANDAS = {
    BigInteger: "Int64",
    Boolean: "bool",
    Float: "float64",
    Integer: "Int32",
    String: "string",
    Date: "datetime64[s]",
    DateTime: "datetime64[s]",
}
"""Type map so we can use the sqlalchemy metadata."""


class PartitionedParquetIOManager(ConfigurableIOManager):
    """An IOManager that writes and retrieves data frames from parquet files.

    It stores partitioned outputs nested under the primary asset key.
    """

    @property
    def _base_path(self):
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Save a data frame to a parquet file."""
        path = self._get_path(context)
        if "://" not in self._base_path:
            path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(obj, pd.DataFrame):
            row_count = len(obj)
            context.log.debug(f"Row count: {row_count}")
            table_name = get_table_name_from_context(context)
            assert (
                table_name in usage_metrics_metadata.tables
            ), f"""{table_name} does not have a schema defined.
                Create a schema for it in usage_metrics.models."""
            table_metadata = usage_metrics_metadata.tables[table_name]
            table_dtypes = {
                c.name: SQLALCHEMY_TO_PANDAS[type(c.type)]
                for c in table_metadata.columns
            }
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
                schema=pa.schema(
                    [(c, PANDAS_TO_ARROW[t]) for c, t in table_dtypes.items()]
                ),
            )
        else:
            raise ValueError(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": str(path)})

    def load_input(self, context) -> pd.DataFrame | str:
        """Load a data frame from a parquet file."""
        path = self._get_path(context)
        return pd.read_parquet(str(path))

    def _get_path(self, context: InputContext | OutputContext) -> UPath:
        """Compute the parquet path for this asset."""
        key = context.asset_key.path[-1]

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y-%m-%d"
            partition_str = start.strftime(dt_format) + "--" + end.strftime(dt_format)
            return UPath(self._base_path) / key / f"{partition_str}.parquet"
        return UPath(self._base_path) / f"{key}.parquet"


class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):
    """Development version of the parquet IO manager which stores files locally."""

    base_path: str

    @property
    def _base_path(self):
        return self.base_path


@io_manager(
    config_schema={
        "base_path": Field(
            str,
            description="Base path for local parquet storage.",
            default_value=str(UPath(os.environ.get("DATA_DIR", ".")) / "usage_metrics"),
        )
    }
)
def local_parquet_manager(init_context) -> LocalPartitionedParquetIOManager:
    """Create LocalPartitionedParquetIOManager dagster resource."""
    return LocalPartitionedParquetIOManager(
        base_path=init_context.resource_config["base_path"]
    )


class GCSPartitionedParquetIOManager(PartitionedParquetIOManager):
    """Prod version of the parquet IO manager which stores files on GCS."""

    gcs_bucket: str

    @property
    def _base_path(self):
        return "gs://" + self.gcs_bucket


@io_manager(
    config_schema={
        "gcs_bucket": Field(
            str,
            description="GCS bucket for remote parquet storage.",
            default_value=os.environ.get("GCS_BUCKET", "metrics.catalyst.coop"),
        )
    }
)
def gcs_parquet_manager(init_context) -> GCSPartitionedParquetIOManager:
    """Create GCSPartitionedParquetIOManager dagster resource."""
    return GCSPartitionedParquetIOManager(
        gcs_bucket=init_context.resource_config["gcs_bucket"]
    )
