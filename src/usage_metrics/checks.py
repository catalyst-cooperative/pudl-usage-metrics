"""Pandera-based schema and primary-key asset checks for usage_metrics tables.

For every table with a pyarrow schema defined in :mod:`usage_metrics.models`,
build an asset check that validates the materialized parquet data against
that schema's column types and (documentation-only, previously unenforced)
primary key, now enforced here as a uniqueness constraint.
"""

import json

import pandera.errors
import pandera.pyarrow as pandera
import pyarrow as pa
from dagster import AssetCheckResult, AssetChecksDefinition, AssetKey, asset_check

from usage_metrics.models import usage_metrics_schemas


def _pandera_schema(schema: pa.Schema) -> pandera.DataFrameSchema:
    """Build a pandera pyarrow DataFrameSchema from a pyarrow Schema.

    Columns are marked nullable, since nothing has enforced non-null
    constraints on this data before now, and we don't want to fail checks on
    legitimately sparse columns. Only the documented primary key is enforced,
    as a uniqueness constraint.
    """
    columns = {
        field.name: pandera.Column(field.type, nullable=True) for field in schema
    }
    primary_key = None
    if schema.metadata and b"primary_key" in schema.metadata:
        primary_key = json.loads(schema.metadata[b"primary_key"])
    return pandera.DataFrameSchema(columns, unique=primary_key, strict=False)


def _make_schema_check(
    table_name: str, arrow_schema: pa.Schema
) -> AssetChecksDefinition:
    """Build an asset check that validates a table against its pyarrow schema."""
    pandera_schema = _pandera_schema(arrow_schema)

    @asset_check(
        asset=AssetKey(table_name),
        name="pandera_schema_check",
        required_resource_keys={"pyarrow_reader"},
    )
    def _check(context) -> AssetCheckResult:
        table = context.resources.pyarrow_reader.read_table(table_name, context)
        try:
            pandera_schema.validate(table, lazy=True)
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
