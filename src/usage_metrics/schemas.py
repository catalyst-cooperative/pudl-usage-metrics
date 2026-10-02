"""Derive the pyarrow schema and pandas dtypes for Parquet output from pandera schemas.

The pandera schemas in :mod:`usage_metrics.models` are the source of truth. Pandera
wraps each column's type in a narwhals dtype, and narwhals can convert its schemas to
pyarrow and pandas, so most of the conversion is done by narwhals.
"""

import narwhals as nw
import pandera.pyarrow as pandera
import pyarrow as pa


def _narwhals_schema(schema: pandera.DataFrameSchema) -> nw.Schema:
    return nw.Schema(
        {name: column.dtype.type for name, column in schema.columns.items()}
    )


def arrow_schema(schema: pandera.DataFrameSchema) -> pa.Schema:
    """Build the pyarrow schema that a table is written to Parquet with.

    Column descriptions become field metadata, and the table description becomes
    schema metadata, so that they are in the Parquet file footer.
    """
    fields = [
        pa.field(
            field.name,
            field.type,
            metadata=(
                {"comment": description}
                if (description := schema.columns[field.name].description)
                else None
            ),
        )
        for field in _narwhals_schema(schema).to_arrow()
    ]
    return pa.schema(fields).with_metadata({"comment": schema.description})


def pandas_dtypes(schema: pandera.DataFrameSchema) -> dict[str, str]:
    """Get the nullable pandas dtype of each column, to cast dataframes before writing."""
    dtypes = _narwhals_schema(schema).to_pandas(dtype_backend="numpy_nullable")
    return {name: str(dtype) for name, dtype in dtypes.items()}
