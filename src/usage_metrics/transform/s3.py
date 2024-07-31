"""Transform data from S3 logs."""

import pandas as pd
from dagster import AssetExecutionContext, Definitions, asset
from usage_metrics.ops.datasette import geocode_ips  # MOVE TO HELPER FUNCTION

FIELD_NAMES = [
    "bucket_owner",
    "bucket",
    "time",
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


@asset
def transform_s3_logs(
    context: AssetExecutionContext,
    extract_s3_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Transform daily S3 logs.

    Add column headers, geocode values,
    """
    # Combine time and timezone columns
    extract_s3_logs[2] = extract_s3_logs[2] + " " + extract_s3_logs[3]
    extract_s3_logs = extract_s3_logs.drop(columns=[3])

    # Name columns
    extract_s3_logs.columns = FIELD_NAMES

    # Geocode IPS
    extract_s3_logs["remote_ip"] = extract_s3_logs["remote_ip"].mask(
        extract_s3_logs["remote_ip"].eq("-"), pd.NA
    )  # Mask null IPs
    geocoded_df = geocode_ips(context, extract_s3_logs)

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


defs = Definitions(
    assets=[transform_s3_logs],
)
