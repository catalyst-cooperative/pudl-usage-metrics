"""Transform data from S3 logs."""

import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)

from usage_metrics.helpers import geocode_ips


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "s3"},
)
def core_s3_logs(
    context: AssetExecutionContext,
    raw_s3_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Transform daily S3 logs.

    Add column headers, geocode values,
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_s3_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_s3_logs
    # Name columns
    raw_s3_logs.columns = [
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

    # Drop entirely duplicate rows
    raw_s3_logs = raw_s3_logs.drop_duplicates()

    # Combine time and timezone columns
    raw_s3_logs.time = raw_s3_logs.time + " " + raw_s3_logs.timezone
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

    # Convert bytes to megabytes
    geocoded_df["bytes_sent"] = geocoded_df["bytes_sent"] / 1000000
    geocoded_df = geocoded_df.rename(columns={"bytes_sent": "megabytes_sent"})

    # Sometimes the request_id is not unique (when data is copied between S3 buckets
    # or for some deletion requests).
    # Let's make an actually unique ID.
    geocoded_df["id"] = (
        geocoded_df.request_id + "_" + geocoded_df.operation + "_" + geocoded_df.key
    )
    geocoded_df = geocoded_df.set_index("id")

    assert geocoded_df.index.is_unique

    # Drop unnecessary geocoding columns
    geocoded_df = geocoded_df.drop(
        columns=[
            "remote_ip_country_flag",
            "remote_ip_country_flag_url",
            "remote_ip_country_currency",
            "remote_ip_continent",
            "remote_ip_isEU",
        ]
    )

    context.log.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} environment.")

    return geocoded_df.reset_index()
