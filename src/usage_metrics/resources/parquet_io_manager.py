"""Dagster parquet IO manager.

Adapted from example at
https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py
"""

import os
from pathlib import Path

import pandas
from dagster import (
    ConfigurableIOManager,
    Field,
    InputContext,
    OutputContext,
    io_manager,
)


class PartitionedParquetIOManager(ConfigurableIOManager):
    """An IOManager that writes and retrieves data frames from parquet files.

    It stores partitioned outputs nested under the primary asset key.
    """

    @property
    def _base_path(self):
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj: pandas.DataFrame):
        """Save a data frame to a parquet file."""
        path = self._get_path(context)
        if "://" not in self._base_path:
            path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(path=path, index=False)
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context) -> pandas.DataFrame | str:
        """Load a data frame from a parquet file."""
        path = self._get_path(context)
        return pandas.read_parquet(path)

    def _get_path(self, context: InputContext | OutputContext):
        """Compute the parquet path for this asset."""
        key = context.asset_key.path[-1]

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"
            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)
            return Path(self._base_path) / key / f"{partition_str}.parquet"
        return Path(self._base_path) / f"{key}.parquet"


class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):
    """Local-development version of the parquet IO manager which stores files locally."""

    base_path: str

    @property
    def _base_path(self):
        return self.base_path


@io_manager(
    config_schema={
        "base_path": Field(
            str,
            description="Base path for local parquet storage.",
            default_value=os.environ.get("DATA_DIR"),
        )
    }
)
def local_parquet_manager(init_context) -> LocalPartitionedParquetIOManager:
    """Create LocalPartitionedParquetIOManager dagster resource."""
    return LocalPartitionedParquetIOManager(
        base_path=init_context.resource_config["base_path"]
    )


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    """Prod version of the parquet IO manager which stores files on S3."""

    s3_bucket: str

    @property
    def _base_path(self):
        return "s3://" + self.s3_bucket


@io_manager(
    config_schema={
        "s3_bucket": Field(
            str,
            description="S3 bucket for remote parquet storage.",
            default_value=os.environ.get("S3_BUCKET", "pudl.catalyst.coop"),
        )
    }
)
def s3_parquet_manager(init_context) -> S3PartitionedParquetIOManager:
    """Create S3PartitionedParquetIOManager dagster resource."""
    return S3PartitionedParquetIOManager(
        s3_bucket=init_context.resource_config["s3_bucket"]
    )
