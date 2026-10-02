"""Test naming of the headerless raw S3 log columns.

S3 access logs have no header, so columns are named by position. These tests pin that
positional contract down in one place, and check that a change to the log format is
caught instead of silently filing values under the wrong names.
"""

import io

import pandas as pd
import pandera.errors
import pyarrow as pa
import pytest

from usage_metrics.core.s3 import S3_LOG_COLUMNS, name_s3_log_columns
from usage_metrics.models import usage_metrics_schemas
from usage_metrics.schemas import arrow_schema

BEFORE_AWS_REGION = "2026-02-15"
AFTER_AWS_REGION = "2026-02-16"

OWNER = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"
# Each field of an S3 access log row, in order. Quoted fields contain spaces.
LOG_FIELDS = {
    "bucket_owner": OWNER,
    "bucket": "pudl.catalyst.coop",
    "time": "[06/Feb/2019:00:00:38",
    "timezone": "+0000]",
    "remote_ip": "192.0.2.3",
    "requester": "-",
    "request_id": "3E57427F3EXAMPLE",
    "operation": "REST.GET.OBJECT",
    "key": "stable/pudl.sqlite.zip",
    "request_uri": '"GET /stable/pudl.sqlite.zip HTTP/1.1"',
    "http_status": "200",
    "error_code": "-",
    "bytes_sent": "1048576",
    "object_size": "2097152",
    "total_time": "7",
    "turn_around_time": "5",
    "referer": '"-"',
    "user_agent": '"duckdb/1.4.0 (linux)"',
    "version_id": "-",
    "host_id": "s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3=",
    "signature_version": "SigV4",
    "cipher_suite": "ECDHE-RSA-AES128-GCM-SHA256",
    "authentication_type": "-",
    "host_header": "pudl.catalyst.coop",
    "tls_version": "TLSv1.3",
    "access_point_arn": "-",
    "acl_required": "-",
}
assert list(LOG_FIELDS) == S3_LOG_COLUMNS


def _read(fields: list[str]) -> pd.DataFrame:
    """Parse one row exactly the way S3Extractor.load_file does."""
    return pd.read_csv(io.StringIO(" ".join(fields)), delimiter=" ", header=None)


def test_each_field_ends_up_under_its_own_name() -> None:
    """The positional contract: the nth field of a row is the nth named column."""
    named = name_s3_log_columns(_read(list(LOG_FIELDS.values())), BEFORE_AWS_REGION)

    assert list(named.columns) == S3_LOG_COLUMNS
    row = named.iloc[0]
    for column, raw in LOG_FIELDS.items():
        # read_csv strips the quotes and turns numeric fields into numbers.
        assert str(row[column]) == raw.strip('"'), column


def test_aws_region_is_dropped_from_the_newer_layout() -> None:
    """From late February 2026 rows have a trailing aws_region field we don't keep."""
    named = name_s3_log_columns(
        _read([*LOG_FIELDS.values(), "us-west-2"]), AFTER_AWS_REGION
    )

    assert list(named.columns) == S3_LOG_COLUMNS
    assert "us-west-2" not in named.iloc[0].astype(str).tolist()


@pytest.mark.parametrize(
    "partition_key,extra_fields",
    [
        (BEFORE_AWS_REGION, ["us-west-2"]),  # too many for the old layout
        (AFTER_AWS_REGION, []),  # too few for the new layout
        (AFTER_AWS_REGION, ["us-west-2", "new-field"]),  # too many
    ],
)
def test_wrong_number_of_columns_is_rejected(partition_key, extra_fields) -> None:
    raw = _read([*LOG_FIELDS.values(), *extra_fields])
    with pytest.raises(ValueError, match="Expected .* columns"):
        name_s3_log_columns(raw, partition_key)


SCHEMA = usage_metrics_schemas["core_s3_logs"]
PATTERN_COLUMNS = [
    "bucket_owner",
    "remote_ip",
    "operation",
    "request_uri",
    "signature_version",
    "tls_version",
]


def _validate_named_columns(named: pd.DataFrame) -> None:
    """Validate the columns of core_s3_logs that have patterns, using its schema.

    The other columns are null, since the transform that fills them in isn't run.
    """
    arrow = arrow_schema(SCHEMA)
    rows = len(named)
    columns = {field.name: pa.array([None] * rows, field.type) for field in arrow}
    columns["id"] = pa.array([f"id-{i}" for i in range(rows)])
    for column in PATTERN_COLUMNS:
        columns[column] = pa.array(named[column].astype(str), pa.string())
    SCHEMA.validate(pa.table(columns, schema=arrow), lazy=True)


def test_correctly_named_columns_pass_the_schema() -> None:
    named = name_s3_log_columns(_read(list(LOG_FIELDS.values())), BEFORE_AWS_REGION)

    _validate_named_columns(named)


@pytest.mark.parametrize("insert_after", ["bucket", "operation", "key", "http_status"])
def test_field_inserted_mid_row_is_rejected_by_the_schema(insert_after) -> None:
    """A field AWS adds mid-row keeps the column count right but shifts every
    later field one column over. Naming can't tell, but the formats declared for
    some columns in the schema can, so that it isn't silently misnamed.
    """
    fields = []
    for column, raw in LOG_FIELDS.items():
        fields.append(raw)
        if column == insert_after:
            fields.append("new-field")
    # One extra field, so this is the right width for the newer layout.
    raw = _read(fields)
    assert raw.shape[1] == len(S3_LOG_COLUMNS) + 1
    named = name_s3_log_columns(raw, AFTER_AWS_REGION)

    with pytest.raises(pandera.errors.SchemaErrors) as error:
        _validate_named_columns(named)

    failed = set(error.value.failure_cases.to_pandas()["column"])
    assert failed and failed <= set(PATTERN_COLUMNS)


def test_input_is_not_modified() -> None:
    raw = _read(list(LOG_FIELDS.values()))
    before = raw.copy()
    name_s3_log_columns(raw, BEFORE_AWS_REGION)
    pd.testing.assert_frame_equal(raw, before)
