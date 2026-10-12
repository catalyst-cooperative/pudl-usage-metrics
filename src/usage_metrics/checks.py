"""Pandera-based schema and primary-key asset checks for usage_metrics tables.

For every table with a pandera schema defined in :mod:`usage_metrics.models`, build an
asset check that validates the materialized parquet data against that schema: its column
types, which columns may be null, and its primary key.
"""

import pandas as pd
import pandera.errors
import pandera.pyarrow as pandera
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetKey,
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
    table_name: str, schema: pandera.DataFrameSchema
) -> AssetChecksDefinition:
    """Build an asset check that validates a table against its pandera schema."""

    @asset_check(
        asset=AssetKey(table_name),
        name="pandera_schema_check",
        required_resource_keys={"pyarrow_reader"},
        blocking=True,
    )
    def _check(context) -> AssetCheckResult:
        table = context.resources.pyarrow_reader.read_table(table_name, context)
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


pandera_schema_checks: list[AssetChecksDefinition] = [
    _make_schema_check(table_name, schema)
    for table_name, schema in usage_metrics_schemas.items()
]
