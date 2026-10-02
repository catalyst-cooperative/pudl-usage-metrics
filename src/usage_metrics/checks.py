"""Pandera-based schema and primary-key asset checks for usage_metrics tables.

For every table with a pandera schema defined in :mod:`usage_metrics.models`, build an
asset check that validates the materialized parquet data against that schema: its column
types, which columns may be null, and its primary key.
"""

import pandera.errors
import pandera.pyarrow as pandera
from dagster import AssetCheckResult, AssetChecksDefinition, AssetKey, asset_check

from usage_metrics.models import usage_metrics_schemas


def _make_schema_check(
    table_name: str, schema: pandera.DataFrameSchema
) -> AssetChecksDefinition:
    """Build an asset check that validates a table against its pandera schema."""

    @asset_check(
        asset=AssetKey(table_name),
        name="pandera_schema_check",
        required_resource_keys={"pyarrow_reader"},
    )
    def _check(context) -> AssetCheckResult:
        table = context.resources.pyarrow_reader.read_table(table_name, context)
        try:
            schema.validate(table, lazy=True)
        except pandera.errors.SchemaErrors as err:
            return AssetCheckResult(
                passed=False,
                metadata={"failure_cases": str(err.failure_cases.to_pandas())},
            )
        return AssetCheckResult(passed=True, metadata={"row_count": table.num_rows})

    return _check


pandera_schema_checks: list[AssetChecksDefinition] = [
    _make_schema_check(table_name, schema)
    for table_name, schema in usage_metrics_schemas.items()
]
