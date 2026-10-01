"""PyArrow schemas for usage_metrics Parquet outputs.

Each table is defined as a :class:`pyarrow.Schema`. Column-level documentation is stored
as field metadata (``comment``), and table-level documentation and the
(documentation-only, unenforced) primary key are stored as schema metadata. This
metadata is written into the Parquet file footer, so it travels with the data and can be
read back with e.g. ``pyarrow.parquet.read_schema(path).metadata``.
"""

import json

import pyarrow as pa

ARROW_TO_PANDAS: dict[pa.DataType, str] = {
    pa.bool_(): "boolean",
    pa.int64(): "Int64",
    pa.float64(): "float64",
    pa.string(): "string",
    pa.timestamp("s"): "datetime64[s]",
}
"""The only pyarrow types schemas may use, and the pandas dtype each maps to.

The Parquet IO manager uses this to cast dataframes before writing.
"""


def _field(name: str, datatype: pa.DataType, comment: str | None = None) -> pa.Field:
    """Build a pa.field with an optional doc comment stored as field metadata.

    Raises:
        ValueError: if ``datatype`` has no entry in ARROW_TO_PANDAS.
    """
    if datatype not in ARROW_TO_PANDAS:
        raise ValueError(
            f"Field {name!r} has type {datatype}, which has no pandas dtype in "
            f"ARROW_TO_PANDAS. Supported types: {list(ARROW_TO_PANDAS)}"
        )
    metadata = {"comment": comment} if comment else None
    return pa.field(name, datatype, metadata=metadata)


def _table_schema(
    fields: list[pa.Field], comment: str, primary_key: list[str] | None = None
) -> pa.Schema:
    """Build a pa.Schema with table-level comment and primary key metadata.

    The primary key is documentation only -- pyarrow/Parquet has no notion of
    a primary key constraint, so nothing enforces or deduplicates on it.
    """
    metadata = {"comment": comment}
    if primary_key:
        metadata["primary_key"] = json.dumps(primary_key)
    return pa.schema(fields).with_metadata(metadata)


# Metadata derived from:
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html#log-record-fields
# https://ipinfo.io/developers/lite-api
core_s3_logs = _table_schema(
    fields=[
        _field(name="id", datatype=pa.string(), comment="A unique ID for each log."),
        _field(
            name="time",
            datatype=pa.timestamp("s"),
            comment="The time at which the request was received; these dates and times are in Coordinated Universal Time (UTC).",
        ),
        _field(
            name="request_uri",
            datatype=pa.string(),
            comment="The Request-URI part of the HTTP request message.",
        ),
        _field(
            name="operation",
            datatype=pa.string(),
            comment="The operation listed here is declared as SOAP.operation, REST.HTTP_method.resource_type, WEBSITE.HTTP_method.resource_type, or BATCH.DELETE.OBJECT, or S3.action.resource_type for S3 Lifecycle and logging. For Compute checksum job requests, the operation is listed as S3.COMPUTE.OBJECT.CHECKSUM.",
        ),
        _field(
            name="bucket",
            datatype=pa.string(),
            comment="The name of the bucket that the request was processed against. If the system receives a malformed request and cannot determine the bucket, the request will not appear in any server access log.",
        ),
        _field(
            name="bucket_owner",
            datatype=pa.string(),
            comment="The canonical user ID of the owner of the source bucket. The canonical user ID is another form of the AWS account ID.",
        ),
        _field(
            name="requester",
            datatype=pa.string(),
            comment="The canonical user ID of the requester, or null for unauthenticated requests. If the requester was an IAM user, this field returns the requester's IAM user name along with the AWS account that the IAM user belongs to. This identifier is the same one used for access control purposes.",
        ),
        _field(
            name="http_status",
            datatype=pa.int64(),
            comment="The numeric HTTP status code of the response.",
        ),
        _field(
            name="megabytes_sent",
            datatype=pa.float64(),
            comment="The total size of the object in question in megabytes.",
        ),
        _field(
            name="normalized_file_downloads",
            datatype=pa.float64(),
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        # IP location
        _field(
            name="remote_ip",
            datatype=pa.string(),
            comment="The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _field(
            name="remote_ip_org",
            datatype=pa.string(),
            comment="IP Organization name, as determined by IPInfo.",
        ),
        _field(
            name="remote_ip_country_name",
            datatype=pa.string(),
            comment="Country where the IP is located, as determined by IPInfo.",
        ),
        _field(
            name="remote_ip_asn",
            datatype=pa.string(),
            comment="Autonomous System Number as determined by IPInfo.",
        ),
        _field(
            name="remote_ip_bogon",
            datatype=pa.bool_(),
            comment="Is the IP address a bogon (bogus or invalid)?",
        ),
        _field(
            name="remote_ip_country",
            datatype=pa.string(),
            comment="ISO 3166 country code of the IP address, as determined by IPInfo.",
        ),
        # Other reported context
        _field(
            name="access_point_arn",
            datatype=pa.string(),
            comment="The Amazon Resource Name (ARN) of the access point of the request. If the access point ARN is malformed or not used, the field will be null",
        ),
        _field(
            name="acl_required",
            datatype=pa.string(),
            comment="A string that indicates whether the request required an access control list (ACL) for authorization. If the request required an ACL for authorization, the string is Yes. If no ACLs were required, the string is -.",
        ),
        _field(
            name="authentication_type",
            datatype=pa.string(),
            comment="The type of request authentication used: AuthHeader for authentication headers, QueryString for query string (presigned URL), or a - for unauthenticated requests.",
        ),
        _field(
            name="cipher_suite",
            datatype=pa.string(),
            comment="The Transport Layer Security (TLS) cipher that was negotiated for an HTTPS request or a - for HTTP.",
        ),
        _field(
            name="error_code",
            datatype=pa.string(),
            comment="The Amazon S3 Error responses of the GET portion of the copy operation, or - if no error occurred.",
        ),
        _field(
            name="host_header",
            datatype=pa.string(),
            comment="The endpoint that was used to connect to Amazon S3.",
        ),
        _field(
            name="host_id",
            datatype=pa.string(),
            comment="The x-amz-id-2 or Amazon S3 extended request ID.",
        ),
        _field(
            name="key",
            datatype=pa.string(),
            comment="The key (object name) of the object being copied, or - if the operation doesn't take a key parameter.",
        ),
        _field(
            name="object_size",
            datatype=pa.float64(),
            comment="The total size of the object in question in bytes.",
        ),
        _field(
            name="request_id",
            datatype=pa.string(),
            comment="A string generated by Amazon S3 to uniquely identify each request. For Compute checksum job requests, the Request ID field displays the associated job ID.",
        ),
        _field(
            name="referer",
            datatype=pa.string(),
            comment="The value of the HTTP Referer header, if present. HTTP user-agents (for example, browsers) typically set this header to the URL of the linking or embedding page when making a request.",
        ),
        _field(
            name="signature_version",
            datatype=pa.string(),
            comment="The signature version, SigV2 or SigV4, that was used to authenticate the request, or a - for unauthenticated requests.",
        ),
        _field(
            name="tls_version",
            datatype=pa.string(),
            comment="The Transport Layer Security (TLS) version negotiated by the client. The value is one of following: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used.",
        ),
        _field(
            name="total_time",
            datatype=pa.int64(),
            comment="The number of milliseconds that the request was in flight from the server's perspective. This value is measured from the time that your request is received to the time that the last byte of the response is sent. Measurements made from the client's perspective might be longer because of network latency.",
        ),
        _field(
            name="turn_around_time",
            datatype=pa.float64(),
            comment="The number of milliseconds that Amazon S3 spent processing your request. This value is measured from the time that the last byte of your request was received until the time that the first byte of the response was sent.",
        ),
        _field(
            name="user_agent",
            datatype=pa.string(),
            comment="The value of the HTTP User-Agent header.",
        ),
        _field(
            name="version_id",
            datatype=pa.string(),
            comment="The version ID in the request, or - if the operation doesn't take a versionId parameter.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment="Cleaned per-request S3 access logs for PUDL's data distribution bucket.",
    primary_key=["id"],
)

out_s3_logs = _table_schema(
    fields=[
        _field(name="id", datatype=pa.string(), comment="A unique ID for each log."),
        _field(
            name="time",
            datatype=pa.timestamp("s"),
            comment="The time at which the request was received; these dates and times are in Coordinated Universal Time (UTC).",
        ),
        _field(
            name="table",
            datatype=pa.string(),
            comment="The PUDL data table accessed by a user.",
        ),
        _field(
            name="version",
            datatype=pa.string(),
            comment="The version of the PUDL database (e.g., stable, nightly, 2026.1) accessed by a user.",
        ),
        _field(
            name="usage_type",
            datatype=pa.string(),
            comment="The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        # IP location
        _field(
            name="remote_ip",
            datatype=pa.string(),
            comment="The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _field(
            name="remote_ip_org",
            datatype=pa.string(),
            comment="IP Organization name, as determined by IPInfo.",
        ),
        _field(
            name="remote_ip_country_name",
            datatype=pa.string(),
            comment="Country where the IP is located, as determined by IPInfo.",
        ),
        _field(
            name="remote_ip_asn",
            datatype=pa.string(),
            comment="Autonomous System Number as determined by IPInfo.",
        ),
        _field(
            name="remote_ip_bogon",
            datatype=pa.bool_(),
            comment="Is the IP address a bogon (bogus or invalid)?",
        ),
        _field(
            name="remote_ip_country",
            datatype=pa.string(),
            comment="ISO 3166 country code of the IP address, as determined by IPInfo.",
        ),
        # Other reported context
        _field(
            name="access_point_arn",
            datatype=pa.string(),
            comment="The Amazon Resource Name (ARN) of the access point of the request. If the access point ARN is malformed or not used, the field will be null",
        ),
        _field(
            name="acl_required",
            datatype=pa.string(),
            comment="A string that indicates whether the request required an access control list (ACL) for authorization. If the request required an ACL for authorization, the string is Yes. If no ACLs were required, the string is -.",
        ),
        _field(
            name="authentication_type",
            datatype=pa.string(),
            comment="The type of request authentication used: AuthHeader for authentication headers, QueryString for query string (presigned URL), or a - for unauthenticated requests.",
        ),
        _field(
            name="megabytes_sent",
            datatype=pa.float64(),
            comment="The total size of the object in question in megabytes.",
        ),
        _field(
            name="normalized_file_downloads",
            datatype=pa.float64(),
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            name="cipher_suite",
            datatype=pa.string(),
            comment="The Transport Layer Security (TLS) cipher that was negotiated for an HTTPS request or a - for HTTP.",
        ),
        _field(
            name="error_code",
            datatype=pa.string(),
            comment="The Amazon S3 Error responses of the GET portion of the copy operation, or - if no error occurred.",
        ),
        _field(
            name="host_header",
            datatype=pa.string(),
            comment="The endpoint that was used to connect to Amazon S3.",
        ),
        _field(
            name="host_id",
            datatype=pa.string(),
            comment="The x-amz-id-2 or Amazon S3 extended request ID.",
        ),
        _field(
            name="http_status",
            datatype=pa.int64(),
            comment="The numeric HTTP status code of the response.",
        ),
        _field(
            name="key",
            datatype=pa.string(),
            comment="The key (object name) of the object being copied, or - if the operation doesn't take a key parameter.",
        ),
        _field(
            name="object_size",
            datatype=pa.float64(),
            comment="The total size of the object in question in bytes.",
        ),
        _field(
            name="referer",
            datatype=pa.string(),
            comment="The value of the HTTP Referer header, if present. HTTP user-agents (for example, browsers) typically set this header to the URL of the linking or embedding page when making a request.",
        ),
        _field(
            name="request_id",
            datatype=pa.string(),
            comment="A string generated by Amazon S3 to uniquely identify each request. For Compute checksum job requests, the Request ID field displays the associated job ID.",
        ),
        _field(
            name="request_uri",
            datatype=pa.string(),
            comment="The Request-URI part of the HTTP request message.",
        ),
        _field(
            name="signature_version",
            datatype=pa.string(),
            comment="The signature version, SigV2 or SigV4, that was used to authenticate the request, or a - for unauthenticated requests.",
        ),
        _field(
            name="tls_version",
            datatype=pa.string(),
            comment="The Transport Layer Security (TLS) version negotiated by the client. The value is one of following: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used.",
        ),
        _field(
            name="total_time",
            datatype=pa.int64(),
            comment="The number of milliseconds that the request was in flight from the server's perspective. This value is measured from the time that your request is received to the time that the last byte of the response is sent. Measurements made from the client's perspective might be longer because of network latency.",
        ),
        _field(
            name="turn_around_time",
            datatype=pa.float64(),
            comment="The number of milliseconds that Amazon S3 spent processing your request. This value is measured from the time that the last byte of your request was received until the time that the first byte of the response was sent.",
        ),
        _field(
            name="user_agent",
            datatype=pa.string(),
            comment="The value of the HTTP User-Agent header.",
        ),
        _field(
            name="version_id",
            datatype=pa.string(),
            comment="The version ID in the request, or - if the operation doesn't take a versionId parameter.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment=(
        "Per-request S3 access logs enriched with IP geolocation and download-type "
        "classification, for final output."
    ),
    primary_key=["id"],
)

out_s3_daily_summary_by_table = _table_schema(
    fields=[
        _field(name="id", datatype=pa.string(), comment="A unique ID for each log."),
        _field(
            name="time",
            datatype=pa.timestamp("s"),
            comment="The day for which metrics are reported.",
        ),
        _field(
            name="table",
            datatype=pa.string(),
            comment="The PUDL data table accessed by a user.",
        ),
        _field(
            name="version",
            datatype=pa.string(),
            comment="The version of the PUDL database (e.g., stable, nightly, 2026.1) accessed by a user.",
        ),
        _field(
            name="usage_type",
            datatype=pa.string(),
            comment="The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        _field(
            name="megabytes_sent",
            datatype=pa.float64(),
            comment="The total size of the object in question in megabytes.",
        ),
        _field(
            name="normalized_file_downloads",
            datatype=pa.float64(),
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            name="request_count",
            datatype=pa.int64(),
            comment="The number of requests made per table, usage method and day.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment="Daily S3 usage totals, aggregated by table and download/usage method.",
    primary_key=["id"],
)

out_s3_daily_summary_by_user = _table_schema(
    fields=[
        _field(name="id", datatype=pa.string(), comment="A unique ID for each log."),
        _field(
            name="time",
            datatype=pa.timestamp("s"),
            comment="The day for which metrics are reported.",
        ),
        _field(
            name="table",
            datatype=pa.string(),
            comment="The PUDL data table accessed by a user.",
        ),
        _field(
            name="version",
            datatype=pa.string(),
            comment="The version of the PUDL database (e.g., stable, nightly, 2026.1) accessed by a user.",
        ),
        _field(
            name="usage_type",
            datatype=pa.string(),
            comment="The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        # IP location
        _field(
            name="remote_ip",
            datatype=pa.string(),
            comment="The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _field(
            name="remote_ip_org",
            datatype=pa.string(),
            comment="IP Organization name, as determined by IPInfo.",
        ),
        _field(
            name="remote_ip_country_name",
            datatype=pa.string(),
            comment="Country where the IP is located, as determined by IPInfo.",
        ),
        _field(
            name="megabytes_sent",
            datatype=pa.float64(),
            comment="The total size of the object in question in megabytes.",
        ),
        _field(
            name="normalized_file_downloads",
            datatype=pa.float64(),
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            name="request_count",
            datatype=pa.int64(),
            comment="The number of requests made per table, usage method and day.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment="Daily S3 usage totals, aggregated by requester IP and download/usage method.",
    primary_key=["id"],
)

out_s3_daily_summary_by_db = _table_schema(
    fields=[
        _field(name="id", datatype=pa.string(), comment="A unique ID for each log."),
        _field(
            name="time",
            datatype=pa.timestamp("s"),
            comment="The day for which metrics are reported.",
        ),
        _field(
            name="database",
            datatype=pa.string(),
            comment="Which type of database the record is accessing (e.g., pudl.sqlite, ferc1.duckdb). Parquet files are lumped into one parquet_file record.",
        ),
        _field(name="version", datatype=pa.string()),
        _field(
            name="usage_type",
            datatype=pa.string(),
            comment="The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        _field(
            name="megabytes_sent",
            datatype=pa.float64(),
            comment="The total size of the object in question in megabytes.",
        ),
        _field(
            name="normalized_file_downloads",
            datatype=pa.float64(),
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            name="request_count",
            datatype=pa.int64(),
            comment="The number of requests made per table, usage method and day.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment=(
        "Daily S3 usage totals, aggregated by database (e.g. pudl.sqlite, "
        "ferc1.duckdb) and download/usage method."
    ),
    primary_key=["id"],
)

core_kaggle_logs = _table_schema(
    fields=[
        _field(
            name="metrics_date",
            datatype=pa.timestamp("s"),
            comment="The unique date for each metrics snapshot.",
        ),
        # Metrics on Kaggle usage
        _field(
            name="total_views",
            datatype=pa.int64(),
            comment="How many people have viewed this dataset all-time.",
        ),
        _field(
            name="total_downloads",
            datatype=pa.int64(),
            comment="How many people have downloaded this dataset all-time.",
        ),
        _field(
            name="total_votes",
            datatype=pa.int64(),
            comment="How many people have upvoted this dataset all-time.",
        ),
        _field(
            name="usability_rating",
            datatype=pa.float64(),
            comment="The current Kaggle usability rating (out of 10).",
        ),
        # Metadata on dataset
        _field(
            name="dataset_name",
            datatype=pa.string(),
            comment="The short-hand name (slug) of the dataset.",
        ),
        _field(name="owner", datatype=pa.string(), comment="The owner of the dataset."),
        _field(
            name="title", datatype=pa.string(), comment="The full title of the dataset."
        ),
        _field(
            name="subtitle",
            datatype=pa.string(),
            comment="The subtitle of the dataset.",
        ),
        _field(
            name="description",
            datatype=pa.string(),
            comment="The description of the dataset.",
        ),
        _field(
            name="keywords",
            datatype=pa.string(),
            comment="All keywords associated with the dataset.",
        ),
        _field(
            name="dataset_id",
            datatype=pa.string(),
            comment="The unique dataset ID generated by Kaggle.",
        ),
        _field(
            name="is_private",
            datatype=pa.string(),
            comment="Whether the dataset is private (not viewable by the public).",
        ),
        _field(
            name="licenses",
            datatype=pa.string(),
            comment="A list of licenses attributed to the dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
        ),
        _field(
            name="collaborators",
            datatype=pa.string(),
            comment="A list of Kaggle users who are listed as collaborators on this dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
        ),
        _field(name="data", datatype=pa.string()),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment="Daily snapshot of PUDL's Kaggle dataset usage and metadata.",
    primary_key=["metrics_date"],
)

# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-sources
core_github_popular_referrers = _table_schema(
    fields=[
        _field(
            name="metrics_date",
            datatype=pa.timestamp("s"),
            comment="The date for each metrics snapshot.",
        ),
        _field(name="referrer", datatype=pa.string(), comment="The unique referrer."),
        _field(
            name="total_referrals",
            datatype=pa.int64(),
            comment="Total number of referrals over the last 14 days.",
        ),
        _field(
            name="unique_referrals",
            datatype=pa.int64(),
            comment="Unique number of referrals over the last 14 days.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment=(
        "Top 10 external referrers to the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date", "referrer"],
)

# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-paths
core_github_popular_paths = _table_schema(
    fields=[
        _field(
            name="metrics_date",
            datatype=pa.timestamp("s"),
            comment="The date for each metrics snapshot.",
        ),
        _field(
            name="path",
            datatype=pa.string(),
            comment="One of the ten most popular Github paths on a given date.",
        ),
        _field(
            name="title", datatype=pa.string(), comment="Full title of the Github path."
        ),
        _field(
            name="total_views",
            datatype=pa.int64(),
            comment="Total views of the path over the last 14 days.",
        ),
        _field(
            name="unique_views",
            datatype=pa.int64(),
            comment="Unique views of the path over the last 14 days.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment=(
        "Top 10 most-viewed paths in the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date", "path"],
)

# See https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones
core_github_clones = _table_schema(
    fields=[
        _field(
            name="metrics_date",
            datatype=pa.timestamp("s"),
            comment="The date for each metrics snapshot.",
        ),
        _field(
            name="total_clones",
            datatype=pa.int64(),
            comment="Total number of clones of the PUDL repository over the last 14 days.",
        ),
        _field(
            name="unique_clones",
            datatype=pa.int64(),
            comment="Unique number of clones of the PUDL repository over the last 14 days.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment=(
        "Daily count of clones of the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date"],
)

# See docs: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-page-views
core_github_views = _table_schema(
    fields=[
        _field(
            name="metrics_date",
            datatype=pa.timestamp("s"),
            comment="The date for each metrics snapshot.",
        ),
        _field(
            name="total_views",
            datatype=pa.int64(),
            comment="Total views of the repository over the last 14 days.",
        ),
        _field(
            name="unique_views",
            datatype=pa.int64(),
            comment="Unique views of the repository over the last 14 days.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment=(
        "Daily count of views of the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date"],
)

# See docs: https://docs.github.com/en/rest/repos/forks
core_github_forks = _table_schema(
    fields=[
        _field(
            name="id",
            datatype=pa.int64(),
            comment="The unique identifier for each fork.",
        ),
        _field(
            name="node_id",
            datatype=pa.string(),
            comment="The global node ID of the fork in Github.",
        ),
        _field(name="name", datatype=pa.string(), comment="Name of fork."),
        _field(
            name="full_name",
            datatype=pa.string(),
            comment="Full name of fork, including repository.",
        ),
        _field(name="private", datatype=pa.bool_(), comment="Is this fork private?"),
        _field(name="owner", datatype=pa.string(), comment="Metadata about the owner."),
        _field(
            name="description", datatype=pa.string(), comment="Description of the fork."
        ),
        _field(
            name="url",
            datatype=pa.string(),
            comment="API link to the forked repoitory.",
        ),
        _field(
            name="created_at",
            datatype=pa.timestamp("s"),
            comment="Time the repository was created, in UTC.",
        ),
        _field(
            name="updated_at",
            datatype=pa.timestamp("s"),
            comment="Time the repository was last updated, in UTC.",
        ),
        _field(
            name="pushed_at",
            datatype=pa.timestamp("s"),
            comment="Time of the last pushed commit, in UTC.",
        ),
        _field(
            name="homepage",
            datatype=pa.string(),
            comment="Home page of the repository.",
        ),
        _field(
            name="size_kb", datatype=pa.int64(), comment="Size in KB of the repository."
        ),
        _field(
            name="stargazers_count",
            datatype=pa.int64(),
            comment="Count of how many people have starred the repository.",
        ),
        _field(
            name="watchers_count",
            datatype=pa.int64(),
            comment="Count of how many people are watching the repository.",
        ),
        _field(name="language", datatype=pa.string(), comment="Repository language."),
        _field(
            name="has_issues",
            datatype=pa.bool_(),
            comment="Does the repository have issues?",
        ),
        _field(
            name="has_projects",
            datatype=pa.bool_(),
            comment="Does the repository have projects?",
        ),
        _field(
            name="has_downloads",
            datatype=pa.bool_(),
            comment="Does the repository have downloads?",
        ),
        _field(
            name="has_wiki",
            datatype=pa.bool_(),
            comment="Does the repository have a wiki?",
        ),
        _field(
            name="has_pages",
            datatype=pa.bool_(),
            comment="Does the repository have pages?",
        ),
        _field(
            name="has_discussions",
            datatype=pa.bool_(),
            comment="Does the repository have discussions?",
        ),
        _field(
            name="has_pull_requests",
            datatype=pa.bool_(),
            comment="Does the repository have pull requests?",
        ),
        _field(
            name="forks_count",
            datatype=pa.int64(),
            comment="Count of forks of the forked repository.",
        ),
        _field(
            name="archived", datatype=pa.bool_(), comment="Is this repository archived?"
        ),
        _field(
            name="disabled", datatype=pa.bool_(), comment="Is this repository disabled?"
        ),
        _field(
            name="license", datatype=pa.string(), comment="License of the repository."
        ),
        _field(
            name="allow_forking",
            datatype=pa.bool_(),
            comment="Does the repository allow forking?",
        ),
        _field(
            name="is_template",
            datatype=pa.bool_(),
            comment="Is the repository a template?",
        ),
        _field(
            name="web_commit_signoff_required",
            datatype=pa.bool_(),
            comment="Does the repository require signoffs for web-based commits?",
        ),
        _field(
            name="topics",
            datatype=pa.string(),
            comment="A list of topics associated with the repository.",
        ),
        _field(
            name="visibility",
            datatype=pa.string(),
            comment="The visibility setting of the repository.",
        ),
        _field(
            name="forks",
            datatype=pa.int64(),
            comment="How many forks are there for this repository?",
        ),
        _field(
            name="open_issues",
            datatype=pa.int64(),
            comment="How many open issues are there in this repository?",
        ),
        _field(
            name="watchers",
            datatype=pa.int64(),
            comment="How many people are watching this repository?",
        ),
        _field(
            name="default_branch",
            datatype=pa.string(),
            comment="The default branch of the repository.",
        ),
        _field(
            name="permissions",
            datatype=pa.string(),
            comment="Permissions settings on the repository.",
        ),
    ],
    comment="Snapshot of all forks of the PUDL GitHub repository.",
    primary_key=["id"],
)

# See docs: https://docs.github.com/en/rest/activity/starring
core_github_stargazers = _table_schema(
    fields=[
        _field(
            name="id",
            datatype=pa.int64(),
            comment="The unique identifier for each stargazer.",
        ),
        _field(
            name="starred_at",
            datatype=pa.timestamp("s"),
            comment="When the user starred the repository, in UTC.",
        ),
        _field(name="login", datatype=pa.string(), comment="Github username."),
        _field(
            name="node_id",
            datatype=pa.string(),
            comment="The global node ID of the fork in Github.",
        ),
        _field(
            name="url", datatype=pa.string(), comment="API link to the user account."
        ),
        _field(
            name="html_url",
            datatype=pa.string(),
            comment="HTML link to the user account.",
        ),
        _field(
            name="followers_url",
            datatype=pa.string(),
            comment="API link to the user's followers.",
        ),
        _field(
            name="following_url",
            datatype=pa.string(),
            comment="API link to a list of users that the user is following.",
        ),
        _field(
            name="gists_url",
            datatype=pa.string(),
            comment="API link to a list of the user's gists.",
        ),
        _field(
            name="starred_url",
            datatype=pa.string(),
            comment="API link to a list of the user's starred repositories.",
        ),
        _field(
            name="subscriptions_url",
            datatype=pa.string(),
            comment="API link to a list of the user's subscriptions.",
        ),
        _field(
            name="organizations_url",
            datatype=pa.string(),
            comment="API link to a list of the user's organizations.",
        ),
        _field(
            name="repos_url",
            datatype=pa.string(),
            comment="API link to a list of the user's repositories.",
        ),
        _field(
            name="events_url",
            datatype=pa.string(),
            comment="API link to a list of the user's events.",
        ),
        _field(
            name="received_events_url",
            datatype=pa.string(),
            comment="API link to a list of the user's received events.",
        ),
        _field(
            name="type", datatype=pa.string(), comment="Type of entity (e.g., user)."
        ),
        _field(
            name="site_admin", datatype=pa.bool_(), comment="Is this user a site admin?"
        ),
    ],
    comment="Snapshot of all users who have starred the PUDL GitHub repository.",
    primary_key=["id"],
)

# See: https://zenodo.org/help/statistics
core_zenodo_logs = _table_schema(
    fields=[
        _field(
            name="metrics_date",
            datatype=pa.timestamp("s"),
            comment="The date when the metadata was reported.",
        ),
        _field(
            name="version",
            datatype=pa.string(),
            comment="The version (e.g. 10.0.0) of the dataset record.",
        ),
        _field(
            name="dataset_slug",
            datatype=pa.string(),
            comment="The shorthand for the dataset being archived. Matches the pudl_archiver repository dataset slugs when the dataset is archived by the PUDL archiver.",
        ),
        _field(
            name="dataset_downloads",
            datatype=pa.int64(),
            comment="The total number of downloads for the entire dataset. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
        ),
        _field(
            name="dataset_unique_downloads",
            datatype=pa.int64(),
            comment="The total number of unique downloads for the entire dataset. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
        ),
        _field(
            name="dataset_views",
            datatype=pa.int64(),
            comment="The total number of views for the entire dataset. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
        ),
        _field(
            name="dataset_unique_views",
            datatype=pa.int64(),
            comment="The total number of unique downloads for the entire dataset. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
        ),
        _field(
            name="version_downloads",
            datatype=pa.int64(),
            comment="The total number of downloads for the version. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
        ),
        _field(
            name="version_unique_downloads",
            datatype=pa.int64(),
            comment="The total number of unique downloads for the version. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
        ),
        _field(
            name="version_views",
            datatype=pa.int64(),
            comment="The total number of views for the version. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
        ),
        _field(
            name="version_unique_views",
            datatype=pa.int64(),
            comment="The total number of unique downloads for the version. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
        ),
        _field(
            name="version_title",
            datatype=pa.string(),
            comment="The name of the version in Zenodo.",
        ),
        _field(
            name="version_id",
            datatype=pa.int64(),
            comment="The unique ID of the Zenodo version. This is identical to the version DOI.",
        ),
        _field(
            name="version_record_id",
            datatype=pa.int64(),
            comment="The record ID of the Zenodo version. This is identical to the version ID.",
        ),
        _field(
            name="concept_record_id",
            datatype=pa.int64(),
            comment="The concept record ID. This is shared between all versions of a record.",
        ),
        _field(
            name="version_creation_date",
            datatype=pa.timestamp("s"),
            comment="The datetime the record was created.",
        ),
        _field(
            name="version_last_modified_date",
            datatype=pa.timestamp("s"),
            comment="The datetime the record was last modified.",
        ),
        _field(
            name="version_last_updated_date",
            datatype=pa.timestamp("s"),
            comment="The datetime the record was last updated.",
        ),
        _field(
            name="version_publication_date",
            datatype=pa.timestamp("s"),
            comment="The date that the version was published.",
        ),
        _field(
            name="version_doi",
            datatype=pa.string(),
            comment="The DOI of the Zenodo version.",
        ),
        _field(
            name="concept_record_doi",
            datatype=pa.string(),
            comment="The DOI of the Zenodo concept record.",
        ),
        _field(
            name="version_doi_url",
            datatype=pa.string(),
            comment="The DOI link of the Zenodo version.",
        ),
        _field(
            name="version_status",
            datatype=pa.string(),
            comment="The status of the Zenodo version.",
        ),
        _field(
            name="version_state",
            datatype=pa.string(),
            comment="The state of the Zenodo version.",
        ),
        _field(
            name="version_submitted",
            datatype=pa.bool_(),
            comment="Is the version submitted?",
        ),
        _field(
            name="version_description",
            datatype=pa.string(),
            comment="The description of the version.",
        ),
        _field(name="partition_key", datatype=pa.string()),
        _field(
            name="software_hash_id",
            datatype=pa.string(),
            comment="A Software Heritage Software Hash ID (SWHID).",
        ),
    ],
    comment=(
        "Daily snapshot of download and view statistics for PUDL's archived "
        "Zenodo datasets and versions."
    ),
    primary_key=["version_id"],
)

core_eel_hole_log_ins = _table_schema(
    fields=[
        _field(
            name="insert_id",
            datatype=pa.string(),
            comment="A unique identifier for the log entry.",
        ),
        _field(
            name="timestamp",
            datatype=pa.timestamp("s"),
            comment="The time the event described by the log entry occurred.",
        ),
        _field(
            name="text_payload",
            datatype=pa.string(),
            comment="Data provided to the logger in a text format. For the viewer, this includes the redirects generated by a user when logging in.",
        ),
        _field(
            name="log_in_query",
            datatype=pa.string(),
            comment="What was the user doing when they were motivated to log in? This mirrors query when the search prompted a log-in event.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment="Log-in events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_searches = _table_schema(
    fields=[
        _field(
            name="insert_id",
            datatype=pa.string(),
            comment="A unique identifier for the log entry.",
        ),
        _field(
            name="user_id",
            datatype=pa.string(),
            comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
        ),
        _field(
            name="user_domain",
            datatype=pa.string(),
            comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
        ),
        _field(
            name="timestamp",
            datatype=pa.timestamp("s"),
            comment="The time the event described by the log entry occurred.",
        ),
        _field(
            name="query",
            datatype=pa.string(),
            comment="What did a user type into the search box? This logs periodically, so multiple partial search queries may be logged as someone is typing.",
        ),
        _field(
            name="url", datatype=pa.string(), comment="What endpoint is a user hitting?"
        ),
        _field(
            name="session_id",
            datatype=pa.string(),
            comment="A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment="Search query events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_hits = _table_schema(
    fields=[
        _field(
            name="insert_id",
            datatype=pa.string(),
            comment="A unique identifier for the log entry.",
        ),
        _field(
            name="timestamp",
            datatype=pa.timestamp("s"),
            comment="The time the event described by the log entry occurred.",
        ),
        _field(
            name="name",
            datatype=pa.string(),
            comment="The name of the PUDL table returned by a search query. Only populated for 'hit' event types.",
        ),
        _field(
            name="score",
            datatype=pa.float64(),
            comment="The table's relevance score based on the provided search query. Only populated for 'hit' event types.",
        ),
        _field(
            name="tags",
            datatype=pa.string(),
            comment="The tags associated with a given table in the search results. Only populated for 'hit' events types.",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment="Search result hit events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)


def _eel_hole_filter_fields(suffix: str) -> list[pa.Field]:
    """Fields describing one DuckDB filter in an eel hole preview/download query.

    The viewer logs up to seven filters per query, with the fields of the first
    unsuffixed and the rest suffixed ``_1`` through ``_6``.
    """
    return [
        _field(
            name=f"params_filters_field_name{suffix}",
            datatype=pa.string(),
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        _field(
            name=f"params_filters_field_type{suffix}",
            datatype=pa.string(),
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        _field(
            name=f"params_filters_operation{suffix}",
            datatype=pa.string(),
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        _field(
            name=f"params_filters_value{suffix}",
            datatype=pa.string(),
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
    ]


_eel_hole_preview_fields = [
    _field(
        name="insert_id",
        datatype=pa.string(),
        comment="A unique identifier for the log entry.",
    ),
    _field(
        name="user_id",
        datatype=pa.string(),
        comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
    ),
    _field(
        name="user_domain",
        datatype=pa.string(),
        comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
    ),
    _field(
        name="timestamp",
        datatype=pa.timestamp("s"),
        comment="The time the event described by the log entry occurred.",
    ),
    _field(
        name="url", datatype=pa.string(), comment="What endpoint is a user hitting?"
    ),
    _field(
        name="params_name",
        datatype=pa.string(),
        comment="The name of the table being queried using DuckDB.",
    ),
    _field(name="params_page", datatype=pa.int64(), comment="The page of the query."),
    _field(
        name="params_per_page",
        datatype=pa.int64(),
        comment="The number of records returned per DuckDB query. This is set by us, so it should be expected to hold constant without our intervention.",
    ),
    *(
        field
        for i in range(7)
        for field in _eel_hole_filter_fields(suffix="" if i == 0 else f"_{i}")
    ),
    _field(
        name="session_id",
        datatype=pa.string(),
        comment="A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
    ),
    _field(name="partition_key", datatype=pa.string()),
]
"""Fields shared by core_eel_hole_previews and core_eel_hole_downloads.

Rather than specifying these fields twice, we define them once and use the list for both tables. This avoids duplication
and keeps the two tables consistent.
"""

core_eel_hole_previews = _table_schema(
    fields=_eel_hole_preview_fields,
    comment=(
        "DuckDB data preview query events recorded by PUDL's data viewer "
        "(the eel hole)."
    ),
    primary_key=["insert_id"],
)

core_eel_hole_downloads = _table_schema(
    fields=_eel_hole_preview_fields,
    comment="Parquet download events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_user_settings_updates = _table_schema(
    fields=[
        _field(
            name="insert_id",
            datatype=pa.string(),
            comment="A unique identifier for the log entry.",
        ),
        _field(
            name="user_id",
            datatype=pa.string(),
            comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
        ),
        _field(
            name="user_domain",
            datatype=pa.string(),
            comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
        ),
        _field(
            name="timestamp",
            datatype=pa.timestamp("s"),
            comment="The time the event described by the log entry occurred.",
        ),
        _field(
            name="accepted",
            datatype=pa.bool_(),
            comment="Has a user accepted the privacy policy?",
        ),
        _field(
            name="newsletter",
            datatype=pa.bool_(),
            comment="Has a user subscribed to the newsletter?",
        ),
        _field(
            name="outreach",
            datatype=pa.bool_(),
            comment="Has a user agreed to be contacted for further discussion about PUDL?",
        ),
        _field(name="partition_key", datatype=pa.string()),
    ],
    comment=(
        "User privacy/newsletter/outreach setting updates recorded by PUDL's "
        "data viewer (the eel hole)."
    ),
    primary_key=["insert_id"],
)

usage_metrics_schemas: dict[str, pa.Schema] = {
    "core_s3_logs": core_s3_logs,
    "out_s3_logs": out_s3_logs,
    "out_s3_daily_summary_by_table": out_s3_daily_summary_by_table,
    "out_s3_daily_summary_by_user": out_s3_daily_summary_by_user,
    "out_s3_daily_summary_by_db": out_s3_daily_summary_by_db,
    "core_kaggle_logs": core_kaggle_logs,
    "core_github_popular_referrers": core_github_popular_referrers,
    "core_github_popular_paths": core_github_popular_paths,
    "core_github_clones": core_github_clones,
    "core_github_views": core_github_views,
    "core_github_forks": core_github_forks,
    "core_github_stargazers": core_github_stargazers,
    "core_zenodo_logs": core_zenodo_logs,
    "core_eel_hole_log_ins": core_eel_hole_log_ins,
    "core_eel_hole_searches": core_eel_hole_searches,
    "core_eel_hole_hits": core_eel_hole_hits,
    "core_eel_hole_previews": core_eel_hole_previews,
    "core_eel_hole_downloads": core_eel_hole_downloads,
    "core_eel_hole_user_settings_updates": core_eel_hole_user_settings_updates,
}
