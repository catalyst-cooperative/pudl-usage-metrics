"""Transform data from S3 logs."""

import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    asset,
)

from usage_metrics.helpers import geocode_ips

# S3 server access logs are headerless and space-delimited, so the only thing that
# says which field is which is its position in the row.
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
S3_LOG_COLUMNS = [
    "bucket_owner",
    "bucket",
    "time",
    "timezone",
    "remote_ip",
    "requester",
    "request_id",
    "operation",
    "key",
    "request_uri",
    "http_status",
    "error_code",
    "bytes_sent",
    "object_size",
    "total_time",
    "turn_around_time",
    "referer",
    "user_agent",
    "version_id",
    "host_id",
    "signature_version",
    "cipher_suite",
    "authentication_type",
    "host_header",
    "tls_version",
    "access_point_arn",
    "acl_required",
]
"""Names of the fields in an S3 access log row, in the order AWS writes them."""

LAST_PARTITION_WITHOUT_AWS_REGION = "2026-02-15"
"""In late February 2026 AWS added an aws_region field to the end of each row.

We don't need it, so it is dropped rather than persisted.
"""


def name_s3_log_columns(raw_s3_logs: pd.DataFrame, partition_key: str) -> pd.DataFrame:
    """Name the columns of headerless raw S3 logs, checking the number of columns.

    Names are assigned by position, so a field AWS inserts anywhere but the end of
    the row would shift every following field into the wrong column. That is caught
    by the formats declared for some columns in :mod:`usage_metrics.models`, which the
    schema asset check validates after the data is written, and by the parsing of
    ``time`` in :func:`core_s3_logs`.

    Args:
        raw_s3_logs: Raw logs, with integer column labels.
        partition_key: The partition date, used to know which layout to expect.

    Returns:
        A copy with named columns, without the unused aws_region column.

    Raises:
        ValueError: If the number of columns is unexpected.
    """
    columns = list(S3_LOG_COLUMNS)
    if pd.to_datetime(partition_key) > pd.to_datetime(
        LAST_PARTITION_WITHOUT_AWS_REGION
    ):
        columns.append("aws_region")
    if raw_s3_logs.shape[1] != len(columns):
        raise ValueError(
            f"Expected {len(columns)} columns in the S3 logs for {partition_key}, "
            f"found {raw_s3_logs.shape[1]}. Has AWS changed the log format?"
        )
    named = raw_s3_logs.set_axis(columns, axis="columns")
    return named.drop(columns=columns[len(S3_LOG_COLUMNS) :])


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="parquet_manager",
    kinds={"parquet"},
    tags={"source": "s3"},
)
def core_s3_logs(
    context: AssetExecutionContext,
    raw_s3_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Transform daily S3 logs.

    Add column headers, geocode values,
    """
    context.log.info(f"Processing data for {context.partition_key}")

    if raw_s3_logs.empty:
        context.log.warning(f"No data found for {context.partition_key}")
        return raw_s3_logs
    raw_s3_logs = name_s3_log_columns(raw_s3_logs, context.partition_key)

    # Combine time and timezone columns.
    # pandas-stubs infers `Series` too generically here to allow str concatenation.
    raw_s3_logs.time = raw_s3_logs.time + " " + raw_s3_logs.timezone  # type: ignore[unsupported-operation]
    raw_s3_logs = raw_s3_logs.drop(columns=["timezone"])

    # Drop S3 lifecycle transitions
    raw_s3_logs = raw_s3_logs.loc[raw_s3_logs.operation != "S3.TRANSITION_INT.OBJECT"]

    # Geocode IPS
    raw_s3_logs["remote_ip"] = raw_s3_logs["remote_ip"].mask(
        raw_s3_logs["remote_ip"].eq("-"), pd.NA
    )  # Mask null IPs
    geocoded_df = geocode_ips(raw_s3_logs)

    # Convert string to datetime using Pandas
    format_string = "[%d/%b/%Y:%H:%M:%S %z]"
    geocoded_df["time"] = pd.to_datetime(geocoded_df.time, format=format_string)

    geocoded_df["bytes_sent"] = geocoded_df["bytes_sent"].mask(
        geocoded_df["bytes_sent"].eq("-"), 0
    )
    numeric_fields = [
        "bytes_sent",
        "http_status",
        "object_size",
        "total_time",
        "turn_around_time",
    ]
    for field in numeric_fields:
        geocoded_df[field] = pd.to_numeric(geocoded_df[field], errors="coerce")

    # Normalize file download count
    geocoded_df["normalized_file_downloads"] = (
        geocoded_df["bytes_sent"] / geocoded_df["object_size"]
    )

    # Convert bytes to megabytes
    geocoded_df["bytes_sent"] = geocoded_df["bytes_sent"] / 1000000
    geocoded_df = geocoded_df.rename(columns={"bytes_sent": "megabytes_sent"})

    # Sometimes the request_id is not unique (when data is copied between S3 buckets
    # or for some deletion requests).
    # Let's make an actually unique ID.
    geocoded_df["id"] = (
        geocoded_df.request_id + "_" + geocoded_df.operation + "_" + geocoded_df.key
    )

    # Make sure all completely duplicate rows dropped
    geocoded_df = geocoded_df.drop_duplicates()

    geocoded_df = geocoded_df.set_index("id")
    assert geocoded_df.index.is_unique

    context.log.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} environment.")

    return geocoded_df.reset_index()
