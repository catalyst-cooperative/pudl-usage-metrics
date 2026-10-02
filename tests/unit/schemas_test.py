"""Test building pandera table schemas and deriving pyarrow and pandas schemas."""

import pandera.errors
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
        _column("count", int),
        _column("ratio", float),
        _column("flag", bool),
    ],
    description="A table.",
    primary_key=["id"],
)


def test_primary_key_columns_are_required_and_others_nullable() -> None:
    assert not SCHEMA.columns["id"].nullable
    assert all(c.nullable for name, c in SCHEMA.columns.items() if name != "id")
    assert SCHEMA.unique == ["id"]


def test_columns_shared_between_tables_are_not_changed_by_a_primary_key() -> None:
    shared = [_column("a", str), _column("b", str)]
    _table_schema("t1", shared, description="keyed on a", primary_key=["a"])
    _table_schema("t2", shared, description="keyed on b", primary_key=["b"])

    assert all(column.nullable for column in shared)


def test_bad_table_definitions_are_rejected() -> None:
    with pytest.raises(
        ValueError, match=r"Table 'bad' has primary key columns \['nope'\]"
    ):
        _table_schema("bad", [_column("a", str)], "t", primary_key=["nope"])
    with pytest.raises(
        ValueError, match=r"Table 'bad' repeats column names: \['a', 'b'\]"
    ):
        _table_schema("bad", [_column(n, str) for n in ["a", "b", "a", "c", "b"]], "t")


def test_arrow_schema_has_types_and_documentation() -> None:
    arrow = arrow_schema(SCHEMA)

    assert arrow.types == [
        pa.string(),
        pa.timestamp("us"),
        pa.int64(),
        pa.float64(),
        pa.bool_(),
    ]
    assert arrow.field("id").metadata == {b"description": b"A unique ID."}
    assert arrow.field("count").metadata is None
    assert arrow.metadata[b"description"] == b"A table."
    # Nulls in primary keys are reported by the asset checks, not refused on write.
    assert all(field.nullable for field in arrow)


def test_pandas_dtypes_are_nullable() -> None:
    assert pandas_dtypes(SCHEMA) == {
        "id": "string",
        "time": "datetime64[us]",
        "count": "Int64",
        "ratio": "Float64",
        "flag": "boolean",
    }


def test_pattern_must_match_the_whole_value_but_not_nulls() -> None:
    schema = _table_schema(
        "t", [_column("tls", str, pattern=r"-|TLSv1\.[0-3]")], description="t"
    )

    def failures(values: list) -> list:
        table = pa.table({"tls": pa.array(values, pa.string())})
        try:
            schema.validate(table, lazy=True)
        except pandera.errors.SchemaErrors as error:
            return error.failure_cases.to_pandas()["failure_case"].tolist()
        return []

    assert failures(["-", "TLSv1.2", None]) == []
    # Not just a prefix, and not just one side of the alternation.
    assert failures(["-garbage", "xTLSv1.2", "TLSv1.2x", "TLSv1.9"]) == [
        "-garbage",
        "xTLSv1.2",
        "TLSv1.2x",
        "TLSv1.9",
    ]


def _table(**columns: list) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(columns.get("id", ["a", "b"]), pa.string()),
            "time": pa.array([None, None], pa.timestamp("us")),
            "count": pa.array(columns.get("count", [1, None]), pa.int64()),
            "ratio": pa.array([None, None], pa.float64()),
            "flag": pa.array([None, None], pa.bool_()),
        }
    )


def test_validation_allows_nulls_outside_the_primary_key() -> None:
    SCHEMA.validate(_table(), lazy=True)


@pytest.mark.parametrize(
    "ids,check",
    [(["a", None], "not_nullable"), (["a", "a"], "multiple_fields_uniqueness")],
    ids=["null_key", "duplicate_key"],
)
def test_validation_rejects_bad_primary_keys(ids, check) -> None:
    with pytest.raises(pandera.errors.SchemaErrors) as error:
        SCHEMA.validate(_table(id=ids), lazy=True)

    assert check in set(error.value.failure_cases.to_pandas()["check"])


@pytest.mark.parametrize("table_name", list(usage_metrics_schemas))
def test_every_table_schema_is_consistent(table_name: str) -> None:
    schema = usage_metrics_schemas[table_name]
    arrow = arrow_schema(schema)

    # Each schema is named for the variable it's assigned to in models.py.
    assert getattr(models, table_name) is schema
    assert arrow.names == list(schema.columns) == list(pandas_dtypes(schema))
    assert schema.description
    assert schema.unique, f"{table_name} has no primary key"
    assert not any(schema.columns[name].nullable for name in schema.unique)
