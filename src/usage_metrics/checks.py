"""Pandera-based schema and primary-key asset checks for usage_metrics tables.

For every table with a pandera schema defined in :mod:`usage_metrics.models`, build an
asset check that validates the materialized parquet data against that schema: its column
types, which columns may be null, and its primary key.
"""

from collections.abc import Iterable

import pandas as pd
import pandera.errors
import pandera.pyarrow as pandera
import pyarrow as pa
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetKey,
    AssetsDefinition,
    PartitionsDefinition,
    asset_check,
)

from usage_metrics.models import usage_metrics_schemas

EXAMPLES_PER_CHECK = 3
"""How many failing values to show for each column and check in a failure report."""


def _failure_report(failure_cases: pd.DataFrame) -> str:
    """Summarize pandera failure cases: one line per column and check, with examples.

    A table can have many thousands of failing values, so this counts them and shows a
    few, rather than listing them all.
    """
    return (
        failure_cases.groupby(["column", "check"], dropna=False)["failure_case"]
        .agg(
            failures="size",
            examples=lambda cases: cases.astype(str).head(EXAMPLES_PER_CHECK).tolist(),
        )
        .reset_index()
        .to_string(index=False)
    )


def _make_schema_check(
    table_name: str,
    schema: pandera.DataFrameSchema,
    partitions_def: PartitionsDefinition | None,
) -> AssetChecksDefinition:
    """Build an asset check that validates a table against its pandera schema.

    The check is partitioned like the asset it checks, so that it validates the
    partition being run, and loads the table through that asset's IO manager. Annotating
    the input as ``pa.Table`` makes :class:`PartitionedParquetIOManager` return the
    Arrow table as it is stored, without a pandas round trip.
    """

    @asset_check(
        asset=AssetKey(table_name),
        name="pandera_schema_check",
        blocking=True,
        partitions_def=partitions_def,
    )
    def _check(
        context: AssetCheckExecutionContext, table: pa.Table
    ) -> AssetCheckResult:
        try:
            schema.validate(table, lazy=True)
        except pandera.errors.SchemaErrors as err:
            failure_cases = err.failure_cases.to_pandas()
            report = _failure_report(failure_cases)
            partition = f" {context.partition_key}" if context.has_partition_key else ""
            context.log.error(f"{table_name}{partition} failed its schema:\n{report}")
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=(
                    f"{table_name}{partition}: {len(failure_cases)} values failed "
                    f"the table's schema. See the 'report' metadata for the columns "
                    "and checks that failed, with examples."
                ),
                metadata={"report": report, "failure_cases": str(failure_cases)},
            )
        return AssetCheckResult(passed=True, metadata={"row_count": table.num_rows})

    return _check


def build_schema_checks(assets: Iterable[object]) -> list[AssetChecksDefinition]:
    """Build a schema check for every asset that has a pandera schema."""
    return [
        _make_schema_check(
            key.to_user_string(),
            usage_metrics_schemas[key.to_user_string()],
            asset.partitions_def,
        )
        for asset in assets
        if isinstance(asset, AssetsDefinition)
        for key in asset.keys
        if key.to_user_string() in usage_metrics_schemas
    ]
