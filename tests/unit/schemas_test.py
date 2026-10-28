"""Test building pandera table schemas and deriving pyarrow and pandas schemas."""

import pandera.errors
import pandera.pyarrow
import pyarrow as pa
import pytest
from pandera.dtypes import Timestamp

from usage_metrics import models
from usage_metrics.models import _column, _table_schema, usage_metrics_schemas
from usage_metrics.schemas import arrow_schema, pandas_dtypes

SCHEMA = _table_schema(
    name="a_table",
    columns=[
        _column("id", str, "A unique ID."),
        _column("time", Timestamp, "When it happened."),
        _column("count", int, "How many."),
        _column("ratio", float, "A ratio."),
        _column("flag", bool, "A flag."),
    ],
    description="A table.",
    primary_key=["id"],
)


def test_primary_key_columns_are_required_and_others_nullable() -> None:
    """Primary key columns are non-nullable and unique together; all others are nullable."""
    assert not SCHEMA.columns["id"].nullable
    assert all(c.nullable for name, c in SCHEMA.columns.items() if name != "id")
    assert SCHEMA.unique == ["id"]


def test_columns_shared_between_tables_are_not_changed_by_a_primary_key() -> None:
    """Making a column part of one table's primary key doesn't affect other tables.

    Columns are shared between table definitions, so each table has to get its own copy.
    """
    shared = [_column("a", str, "A."), _column("b", str, "B.")]
    _table_schema("t1", shared, description="keyed on a", primary_key=["a"])
    _table_schema("t2", shared, description="keyed on b", primary_key=["b"])

    assert all(column.nullable for column in shared)


def test_bad_table_definitions_are_rejected() -> None:
    """Unnamed or repeated columns, and primary keys that aren't columns, raise ValueError."""
    with pytest.raises(ValueError, match="Table 'bad' has columns without a name"):
        _table_schema("bad", [pandera.pyarrow.Column(str)], "t")
    with pytest.raises(
        ValueError, match=r"Table 'bad' has primary key columns \['nope'\]"
    ):
        _table_schema("bad", [_column("a", str, "A.")], "t", primary_key=["nope"])
    with pytest.raises(
        ValueError, match=r"Table 'bad' repeats column names: \['a', 'b'\]"
    ):
        _table_schema(
            "bad", [_column(n, str, n) for n in ["a", "b", "a", "c", "b"]], "t"
        )


def test_arrow_schema_has_types_and_documentation() -> None:
    """The Arrow schema has the column types, with the descriptions as metadata.

    Column descriptions are field metadata and the table description is schema metadata, so that they are written into the Parquet file footer.
    """
    arrow = arrow_schema(SCHEMA)

    assert arrow.types == [
        pa.string(),
        pa.timestamp("us"),
        pa.int64(),
        pa.float64(),
        pa.bool_(),
    ]
    assert arrow.field("id").metadata == {b"description": b"A unique ID."}
    assert arrow.field("count").metadata == {b"description": b"How many."}
    assert arrow.metadata[b"description"] == b"A table."
    # Nulls in primary keys are reported by the asset checks, not refused on write.
    assert all(field.nullable for field in arrow)


def test_pandas_dtypes_are_nullable() -> None:
    """Each column gets the nullable pandas dtype for its type, so nulls survive in any column."""
    assert pandas_dtypes(SCHEMA) == {
        "id": "string",
        "time": "datetime64[us]",
        "count": "Int64",
        "ratio": "Float64",
        "flag": "boolean",
    }


def test_pattern_must_match_the_whole_value_but_not_nulls() -> None:
    r"""A column's pattern has to match its whole value, and nulls are exempt from it.

    The pattern is `-|TLSv1\.[0-3]`, with two alternatives at the top level. The values
    that must fail each go wrong in a different way: `-garbage` and `TLSv1.2x` have
    extra characters after a match, `xTLSv1.2` has them before it, and `TLSv1.9` has
    the right shape but a digit outside the range. Together they catch a check that
    matches only a prefix, or that applies its anchors to just one alternative.
    """
    schema = _table_schema(
        "t",
        [_column("tls", str, "TLS version.", pattern=r"-|TLSv1\.[0-3]")],
        description="t",
    )

    def failures(values: list) -> list:
        table = pa.table({"tls": pa.array(values, pa.string())})
        try:
            schema.validate(table, lazy=True)
        except pandera.errors.SchemaErrors as error:
            return error.failure_cases.to_pandas()["failure_case"].tolist()
        return []

    assert failures(["-", "TLSv1.2", None]) == []
    assert failures(["-garbage", "xTLSv1.2", "TLSv1.2x", "TLSv1.9"]) == [
        "-garbage",
        "xTLSv1.2",
        "TLSv1.2x",
        "TLSv1.9",
    ]


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_every_table_schema_is_consistent(table_name: str) -> None:
    """Every table schema is documented, named for its variable, and has a valid primary key."""
    schema = usage_metrics_schemas[table_name]
    # Each schema is named for the variable it's assigned to in models.py.
    assert getattr(models, table_name) is schema
    assert schema.description
    undescribed = [name for name, col in schema.columns.items() if not col.description]
    assert not undescribed, f"{table_name} columns have no description: {undescribed}"
    assert schema.unique, f"{table_name} has no primary key"
    assert not any(schema.columns[name].nullable for name in schema.unique)
