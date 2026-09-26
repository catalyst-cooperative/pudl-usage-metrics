"""PyArrow schemas for usage_metrics Parquet outputs.

Each table is defined as a :class:`pyarrow.Schema`. Column-level documentation is stored
as field metadata (``comment``), and table-level documentation and the
(documentation-only, unenforced) primary key are stored as schema metadata. This
metadata is written into the Parquet file footer, so it travels with the data and can be
read back with e.g. ``pyarrow.parquet.read_schema(path).metadata``.
"""

import json

import pyarrow as pa


def _field(name: str, type_: pa.DataType, comment: str | None = None) -> pa.Field:
    """Build a pa.field with an optional doc comment stored as field metadata."""
    metadata = {"comment": comment} if comment else None
    return pa.field(name, type_, metadata=metadata)


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
    [
        _field("id", pa.string(), "A unique ID for each log."),
        _field(
            "time",
            pa.timestamp("s"),
            "The time at which the request was received; these dates and times are in Coordinated Universal Time (UTC).",
        ),
        _field(
            "request_uri",
            pa.string(),
            "The Request-URI part of the HTTP request message.",
        ),
        _field(
            "operation",
            pa.string(),
            "The operation listed here is declared as SOAP.operation, REST.HTTP_method.resource_type, WEBSITE.HTTP_method.resource_type, or BATCH.DELETE.OBJECT, or S3.action.resource_type for S3 Lifecycle and logging. For Compute checksum job requests, the operation is listed as S3.COMPUTE.OBJECT.CHECKSUM.",
        ),
        _field(
            "bucket",
            pa.string(),
            "The name of the bucket that the request was processed against. If the system receives a malformed request and cannot determine the bucket, the request will not appear in any server access log.",
        ),
        _field(
            "bucket_owner",
            pa.string(),
            "The canonical user ID of the owner of the source bucket. The canonical user ID is another form of the AWS account ID.",
        ),
        _field(
            "requester",
            pa.string(),
            "The canonical user ID of the requester, or null for unauthenticated requests. If the requester was an IAM user, this field returns the requester's IAM user name along with the AWS account that the IAM user belongs to. This identifier is the same one used for access control purposes.",
        ),
        _field(
            "http_status",
            pa.int64(),
            "The numeric HTTP status code of the response.",
        ),
        _field(
            "megabytes_sent",
            pa.float64(),
            "The total size of the object in question in megabytes.",
        ),
        _field(
            "normalized_file_downloads",
            pa.float64(),
            "The proportion of the file that is downloaded (0 to 1).",
        ),
        # IP location
        _field(
            "remote_ip",
            pa.string(),
            "The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _field(
            "remote_ip_city",
            pa.string(),
            "City where the IP is located, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_loc",
            pa.string(),
            "Geospatial coordinates of the IP, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_org",
            pa.string(),
            "IP Organization name, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_hostname",
            pa.string(),
            "Name of the IP host, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_country_name",
            pa.string(),
            "Country where the IP is located, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_asn",
            pa.string(),
            "Autonomous System Number as determined by IPInfo.",
        ),
        _field(
            "remote_ip_bogon",
            pa.bool_(),
            "Is the IP address a bogon (bogus or invalid)?",
        ),
        _field(
            "remote_ip_country",
            pa.string(),
            "ISO 3166 country code of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_timezone",
            pa.string(),
            "Timezone of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_latitude",
            pa.float64(),
            "Latitude of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_longitude",
            pa.float64(),
            "Longitude of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_postal",
            pa.string(),
            "Postcode or zipcode of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_region",
            pa.string(),
            "Region/state of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_full_location",
            pa.string(),
            "Full address of the IP address, as determined by IPInfo.",
        ),
        # Other reported context
        _field(
            "access_point_arn",
            pa.string(),
            "The Amazon Resource Name (ARN) of the access point of the request. If the access point ARN is malformed or not used, the field will be null",
        ),
        _field(
            "acl_required",
            pa.string(),
            "A string that indicates whether the request required an access control list (ACL) for authorization. If the request required an ACL for authorization, the string is Yes. If no ACLs were required, the string is -.",
        ),
        _field(
            "authentication_type",
            pa.string(),
            "The type of request authentication used: AuthHeader for authentication headers, QueryString for query string (presigned URL), or a - for unauthenticated requests.",
        ),
        _field(
            "cipher_suite",
            pa.string(),
            "The Transport Layer Security (TLS) cipher that was negotiated for an HTTPS request or a - for HTTP.",
        ),
        _field(
            "error_code",
            pa.string(),
            "The Amazon S3 Error responses of the GET portion of the copy operation, or - if no error occurred.",
        ),
        _field(
            "host_header",
            pa.string(),
            "The endpoint that was used to connect to Amazon S3.",
        ),
        _field(
            "host_id",
            pa.string(),
            "The x-amz-id-2 or Amazon S3 extended request ID.",
        ),
        _field(
            "key",
            pa.string(),
            "The key (object name) of the object being copied, or - if the operation doesn't take a key parameter.",
        ),
        _field(
            "object_size",
            pa.float64(),
            "The total size of the object in question in bytes.",
        ),
        _field(
            "request_id",
            pa.string(),
            "A string generated by Amazon S3 to uniquely identify each request. For Compute checksum job requests, the Request ID field displays the associated job ID.",
        ),
        _field(
            "referer",
            pa.string(),
            "The value of the HTTP Referer header, if present. HTTP user-agents (for example, browsers) typically set this header to the URL of the linking or embedding page when making a request.",
        ),
        _field(
            "signature_version",
            pa.string(),
            "The signature version, SigV2 or SigV4, that was used to authenticate the request, or a - for unauthenticated requests.",
        ),
        _field(
            "tls_version",
            pa.string(),
            "The Transport Layer Security (TLS) version negotiated by the client. The value is one of following: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used.",
        ),
        _field(
            "total_time",
            pa.int64(),
            "The number of milliseconds that the request was in flight from the server's perspective. This value is measured from the time that your request is received to the time that the last byte of the response is sent. Measurements made from the client's perspective might be longer because of network latency.",
        ),
        _field(
            "turn_around_time",
            pa.float64(),
            "The number of milliseconds that Amazon S3 spent processing your request. This value is measured from the time that the last byte of your request was received until the time that the first byte of the response was sent.",
        ),
        _field("user_agent", pa.string(), "The value of the HTTP User-Agent header."),
        _field(
            "version_id",
            pa.string(),
            "The version ID in the request, or - if the operation doesn't take a versionId parameter.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment="Cleaned per-request S3 access logs for PUDL's data distribution bucket.",
    primary_key=["id"],
)

out_s3_logs = _table_schema(
    [
        _field("id", pa.string(), "A unique ID for each log."),
        _field(
            "time",
            pa.timestamp("s"),
            "The time at which the request was received; these dates and times are in Coordinated Universal Time (UTC).",
        ),
        _field("table", pa.string()),
        _field("version", pa.string()),
        _field(
            "usage_type",
            pa.string(),
            "The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        # IP location
        _field(
            "remote_ip",
            pa.string(),
            "The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _field(
            "remote_ip_city",
            pa.string(),
            "City where the IP is located, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_loc",
            pa.string(),
            "Geospatial coordinates of the IP, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_org",
            pa.string(),
            "IP Organization name, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_hostname",
            pa.string(),
            "Name of the IP host, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_country_name",
            pa.string(),
            "Country where the IP is located, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_asn",
            pa.string(),
            "Autonomous System Number as determined by IPInfo.",
        ),
        _field(
            "remote_ip_bogon",
            pa.bool_(),
            "Is the IP address a bogon (bogus or invalid)?",
        ),
        _field(
            "remote_ip_country",
            pa.string(),
            "ISO 3166 country code of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_timezone",
            pa.string(),
            "Timezone of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_latitude",
            pa.float64(),
            "Latitude of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_longitude",
            pa.float64(),
            "Longitude of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_postal",
            pa.string(),
            "Postcode or zipcode of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_region",
            pa.string(),
            "Region/state of the IP address, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_full_location",
            pa.string(),
            "Full address of the IP address, as determined by IPInfo.",
        ),
        # Other reported context
        _field(
            "access_point_arn",
            pa.string(),
            "The Amazon Resource Name (ARN) of the access point of the request. If the access point ARN is malformed or not used, the field will be null",
        ),
        _field(
            "acl_required",
            pa.string(),
            "A string that indicates whether the request required an access control list (ACL) for authorization. If the request required an ACL for authorization, the string is Yes. If no ACLs were required, the string is -.",
        ),
        _field(
            "authentication_type",
            pa.string(),
            "The type of request authentication used: AuthHeader for authentication headers, QueryString for query string (presigned URL), or a - for unauthenticated requests.",
        ),
        _field(
            "megabytes_sent",
            pa.float64(),
            "The total size of the object in question in megabytes.",
        ),
        _field(
            "normalized_file_downloads",
            pa.float64(),
            "The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            "cipher_suite",
            pa.string(),
            "The Transport Layer Security (TLS) cipher that was negotiated for an HTTPS request or a - for HTTP.",
        ),
        _field(
            "error_code",
            pa.string(),
            "The Amazon S3 Error responses of the GET portion of the copy operation, or - if no error occurred.",
        ),
        _field(
            "host_header",
            pa.string(),
            "The endpoint that was used to connect to Amazon S3.",
        ),
        _field(
            "host_id",
            pa.string(),
            "The x-amz-id-2 or Amazon S3 extended request ID.",
        ),
        _field(
            "http_status",
            pa.int64(),
            "The numeric HTTP status code of the response.",
        ),
        _field(
            "key",
            pa.string(),
            "The key (object name) of the object being copied, or - if the operation doesn't take a key parameter.",
        ),
        _field(
            "object_size",
            pa.float64(),
            "The total size of the object in question in bytes.",
        ),
        _field(
            "referer",
            pa.string(),
            "The value of the HTTP Referer header, if present. HTTP user-agents (for example, browsers) typically set this header to the URL of the linking or embedding page when making a request.",
        ),
        _field(
            "request_id",
            pa.string(),
            "A string generated by Amazon S3 to uniquely identify each request. For Compute checksum job requests, the Request ID field displays the associated job ID.",
        ),
        _field(
            "request_uri",
            pa.string(),
            "The Request-URI part of the HTTP request message.",
        ),
        _field(
            "signature_version",
            pa.string(),
            "The signature version, SigV2 or SigV4, that was used to authenticate the request, or a - for unauthenticated requests.",
        ),
        _field(
            "tls_version",
            pa.string(),
            "The Transport Layer Security (TLS) version negotiated by the client. The value is one of following: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used.",
        ),
        _field(
            "total_time",
            pa.int64(),
            "The number of milliseconds that the request was in flight from the server's perspective. This value is measured from the time that your request is received to the time that the last byte of the response is sent. Measurements made from the client's perspective might be longer because of network latency.",
        ),
        _field(
            "turn_around_time",
            pa.float64(),
            "The number of milliseconds that Amazon S3 spent processing your request. This value is measured from the time that the last byte of your request was received until the time that the first byte of the response was sent.",
        ),
        _field("user_agent", pa.string(), "The value of the HTTP User-Agent header."),
        _field(
            "version_id",
            pa.string(),
            "The version ID in the request, or - if the operation doesn't take a versionId parameter.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment=(
        "Per-request S3 access logs enriched with IP geolocation and download-type "
        "classification, for final output."
    ),
    primary_key=["id"],
)

out_s3_daily_summary_by_table = _table_schema(
    [
        _field("id", pa.string(), "A unique ID for each log."),
        _field("time", pa.timestamp("s"), "The day for which metrics are reported."),
        _field("table", pa.string()),
        _field("version", pa.string()),
        _field(
            "usage_type",
            pa.string(),
            "The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        _field(
            "megabytes_sent",
            pa.float64(),
            "The total size of the object in question in megabytes.",
        ),
        _field(
            "normalized_file_downloads",
            pa.float64(),
            "The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            "request_count",
            pa.int64(),
            "The number of requests made per table, usage method and day.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment="Daily S3 usage totals, aggregated by table and download/usage method.",
    primary_key=["id"],
)

out_s3_daily_summary_by_user = _table_schema(
    [
        _field("id", pa.string(), "A unique ID for each log."),
        _field("time", pa.timestamp("s"), "The day for which metrics are reported."),
        _field("table", pa.string()),
        _field("version", pa.string()),
        _field(
            "usage_type",
            pa.string(),
            "The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        # IP location
        _field(
            "remote_ip",
            pa.string(),
            "The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _field(
            "remote_ip_org",
            pa.string(),
            "IP Organization name, as determined by IPInfo.",
        ),
        _field(
            "remote_ip_country_name",
            pa.string(),
            "Country where the IP is located, as determined by IPInfo.",
        ),
        _field(
            "megabytes_sent",
            pa.float64(),
            "The total size of the object in question in megabytes.",
        ),
        _field(
            "normalized_file_downloads",
            pa.float64(),
            "The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            "request_count",
            pa.int64(),
            "The number of requests made per table, usage method and day.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment="Daily S3 usage totals, aggregated by requester IP and download/usage method.",
    primary_key=["id"],
)

out_s3_daily_summary_by_db = _table_schema(
    [
        _field("id", pa.string(), "A unique ID for each log."),
        _field("time", pa.timestamp("s"), "The day for which metrics are reported."),
        _field(
            "database",
            pa.string(),
            "Which type of database the record is accessing (e.g., pudl.sqlite, ferc1.duckdb). Parquet files are lumped into one parquet_file record.",
        ),
        _field("version", pa.string()),
        _field(
            "usage_type",
            pa.string(),
            "The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        _field(
            "megabytes_sent",
            pa.float64(),
            "The total size of the object in question in megabytes.",
        ),
        _field(
            "normalized_file_downloads",
            pa.float64(),
            "The proportion of the file that is downloaded (0 to 1).",
        ),
        _field(
            "request_count",
            pa.int64(),
            "The number of requests made per table, usage method and day.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment=(
        "Daily S3 usage totals, aggregated by database (e.g. pudl.sqlite, "
        "ferc1.duckdb) and download/usage method."
    ),
    primary_key=["id"],
)

core_kaggle_logs = _table_schema(
    [
        _field(
            "metrics_date",
            pa.timestamp("s"),
            "The unique date for each metrics snapshot.",
        ),
        # Metrics on Kaggle usage
        _field(
            "total_views",
            pa.int64(),
            "How many people have viewed this dataset all-time.",
        ),
        _field(
            "total_downloads",
            pa.int64(),
            "How many people have downloaded this dataset all-time.",
        ),
        _field(
            "total_votes",
            pa.int64(),
            "How many people have upvoted this dataset all-time.",
        ),
        _field(
            "usability_rating",
            pa.float64(),
            "The current Kaggle usability rating (out of 10).",
        ),
        # Metadata on dataset
        _field(
            "dataset_name",
            pa.string(),
            "The short-hand name (slug) of the dataset.",
        ),
        _field("owner", pa.string(), "The owner of the dataset."),
        _field("title", pa.string(), "The full title of the dataset."),
        _field("subtitle", pa.string(), "The subtitle of the dataset."),
        _field("description", pa.string(), "The description of the dataset."),
        _field("keywords", pa.string(), "All keywords associated with the dataset."),
        _field(
            "dataset_id",
            pa.string(),
            "The unique dataset ID generated by Kaggle.",
        ),
        _field(
            "is_private",
            pa.string(),
            "Whether the dataset is private (not viewable by the public).",
        ),
        _field(
            "licenses",
            pa.string(),
            "A list of licenses attributed to the dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
        ),
        _field(
            "collaborators",
            pa.string(),
            "A list of Kaggle users who are listed as collaborators on this dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
        ),
        _field("data", pa.string()),
        _field("partition_key", pa.string()),
    ],
    comment="Daily snapshot of PUDL's Kaggle dataset usage and metadata.",
    primary_key=["metrics_date"],
)

# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-sources
core_github_popular_referrers = _table_schema(
    [
        _field(
            "metrics_date",
            pa.timestamp("s"),
            "The date for each metrics snapshot.",
        ),
        _field("referrer", pa.string(), "The unique referrer."),
        _field(
            "total_referrals",
            pa.int64(),
            "Total number of referrals over the last 14 days.",
        ),
        _field(
            "unique_referrals",
            pa.int64(),
            "Unique number of referrals over the last 14 days.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment=(
        "Top 10 external referrers to the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date", "referrer"],
)

# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-paths
core_github_popular_paths = _table_schema(
    [
        _field(
            "metrics_date",
            pa.timestamp("s"),
            "The date for each metrics snapshot.",
        ),
        _field(
            "path",
            pa.string(),
            "One of the ten most popular Github paths on a given date.",
        ),
        _field("title", pa.string(), "Full title of the Github path."),
        _field(
            "total_views",
            pa.int64(),
            "Total views of the path over the last 14 days.",
        ),
        _field(
            "unique_views",
            pa.int64(),
            "Unique views of the path over the last 14 days.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment=(
        "Top 10 most-viewed paths in the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date", "path"],
)

# See https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones
core_github_clones = _table_schema(
    [
        _field(
            "metrics_date",
            pa.timestamp("s"),
            "The date for each metrics snapshot.",
        ),
        _field(
            "total_clones",
            pa.int64(),
            "Total number of clones of the PUDL repository over the last 14 days.",
        ),
        _field(
            "unique_clones",
            pa.int64(),
            "Unique number of clones of the PUDL repository over the last 14 days.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment=(
        "Daily count of clones of the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date"],
)

# See docs: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-page-views
core_github_views = _table_schema(
    [
        _field(
            "metrics_date",
            pa.timestamp("s"),
            "The date for each metrics snapshot.",
        ),
        _field(
            "total_views",
            pa.int64(),
            "Total views of the repository over the last 14 days.",
        ),
        _field(
            "unique_views",
            pa.int64(),
            "Unique views of the repository over the last 14 days.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment=(
        "Daily count of views of the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date"],
)

# See docs: https://docs.github.com/en/rest/repos/forks
core_github_forks = _table_schema(
    [
        _field("id", pa.int64(), "The unique identifier for each fork."),
        _field("node_id", pa.string(), "The global node ID of the fork in Github."),
        _field("name", pa.string(), "Name of fork."),
        _field("full_name", pa.string(), "Full name of fork, including repository."),
        _field("private", pa.bool_(), "Is this fork private?"),
        _field("owner", pa.string(), "Metadata about the owner."),
        _field("description", pa.string(), "Description of the fork."),
        _field("url", pa.string(), "API link to the forked repoitory."),
        _field(
            "created_at",
            pa.timestamp("s"),
            "Time the repository was created, in UTC.",
        ),
        _field(
            "updated_at",
            pa.timestamp("s"),
            "Time the repository was last updated, in UTC.",
        ),
        _field(
            "pushed_at",
            pa.timestamp("s"),
            "Time of the last pushed commit, in UTC.",
        ),
        _field("homepage", pa.string(), "Home page of the repository."),
        _field("size_kb", pa.int64(), "Size in KB of the repository."),
        _field(
            "stargazers_count",
            pa.int64(),
            "Count of how many people have starred the repository.",
        ),
        _field(
            "watchers_count",
            pa.int64(),
            "Count of how many people are watching the repository.",
        ),
        _field("language", pa.string(), "Repository language."),
        _field("has_issues", pa.bool_(), "Does the repository have issues?"),
        _field("has_projects", pa.bool_(), "Does the repository have projects?"),
        _field("has_downloads", pa.bool_(), "Does the repository have downloads?"),
        _field("has_wiki", pa.bool_(), "Does the repository have a wiki?"),
        _field("has_pages", pa.bool_(), "Does the repository have pages?"),
        _field("has_discussions", pa.bool_(), "Does the repository have discussions?"),
        _field(
            "has_pull_requests",
            pa.bool_(),
            "Does the repository have pull requests?",
        ),
        _field("forks_count", pa.int64(), "Count of forks of the forked repository."),
        _field("archived", pa.bool_(), "Is this repository archived?"),
        _field("disabled", pa.bool_(), "Is this repository disabled?"),
        _field("license", pa.string(), "License of the repository."),
        _field("allow_forking", pa.bool_(), "Does the repository allow forking?"),
        _field("is_template", pa.bool_(), "Is the repository a template?"),
        _field(
            "web_commit_signoff_required",
            pa.bool_(),
            "Does the repository require signoffs for web-based commits?",
        ),
        _field(
            "topics",
            pa.string(),
            "A list of topics associated with the repository.",
        ),
        _field("visibility", pa.string(), "The visibility setting of the repository."),
        _field("forks", pa.int64(), "How many forks are there for this repository?"),
        _field(
            "open_issues",
            pa.int64(),
            "How many open issues are there in this repository?",
        ),
        _field(
            "watchers",
            pa.int64(),
            "How many people are watching this repository?",
        ),
        _field("default_branch", pa.string(), "The default branch of the repository."),
        _field("permissions", pa.string(), "Permissions settings on the repository."),
    ],
    comment="Snapshot of all forks of the PUDL GitHub repository.",
    primary_key=["id"],
)

# See docs: https://docs.github.com/en/rest/activity/starring
core_github_stargazers = _table_schema(
    [
        _field(
            "id",
            pa.int64(),
            "The unique identifier for each stargazer.",
        ),
        _field(
            "starred_at",
            pa.timestamp("s"),
            "When the user starred the repository, in UTC.",
        ),
        _field("login", pa.string(), "Github username."),
        _field("node_id", pa.string(), "The global node ID of the fork in Github."),
        _field("url", pa.string(), "API link to the user account."),
        _field("html_url", pa.string(), "HTML link to the user account."),
        _field("followers_url", pa.string(), "API link to the user's followers."),
        _field(
            "following_url",
            pa.string(),
            "API link to a list of users that the user is following.",
        ),
        _field("gists_url", pa.string(), "API link to a list of the user's gists."),
        _field(
            "starred_url",
            pa.string(),
            "API link to a list of the user's starred repositories.",
        ),
        _field(
            "subscriptions_url",
            pa.string(),
            "API link to a list of the user's subscriptions.",
        ),
        _field(
            "organizations_url",
            pa.string(),
            "API link to a list of the user's organizations.",
        ),
        _field(
            "repos_url",
            pa.string(),
            "API link to a list of the user's repositories.",
        ),
        _field("events_url", pa.string(), "API link to a list of the user's events."),
        _field(
            "received_events_url",
            pa.string(),
            "API link to a list of the user's received events.",
        ),
        _field("type", pa.string(), "Type of entity (e.g., user)."),
        _field("site_admin", pa.bool_(), "Is this user a site admin?"),
    ],
    comment="Snapshot of all users who have starred the PUDL GitHub repository.",
    primary_key=["id"],
)

# See: https://zenodo.org/help/statistics
core_zenodo_logs = _table_schema(
    [
        _field(
            "metrics_date",
            pa.timestamp("s"),
            "The date when the metadata was reported.",
        ),
        _field(
            "version",
            pa.string(),
            "The version (e.g. 10.0.0) of the dataset record.",
        ),
        _field(
            "dataset_slug",
            pa.string(),
            "The shorthand for the dataset being archived. Matches the pudl_archiver repository dataset slugs when the dataset is archived by the PUDL archiver.",
        ),
        _field(
            "dataset_downloads",
            pa.int64(),
            "The total number of downloads for the entire dataset. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
        ),
        _field(
            "dataset_unique_downloads",
            pa.int64(),
            "The total number of unique downloads for the entire dataset. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
        ),
        _field(
            "dataset_views",
            pa.int64(),
            "The total number of views for the entire dataset. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
        ),
        _field(
            "dataset_unique_views",
            pa.int64(),
            "The total number of unique downloads for the entire dataset. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
        ),
        _field(
            "version_downloads",
            pa.int64(),
            "The total number of downloads for the version. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
        ),
        _field(
            "version_unique_downloads",
            pa.int64(),
            "The total number of unique downloads for the version. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
        ),
        _field(
            "version_views",
            pa.int64(),
            "The total number of views for the version. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
        ),
        _field(
            "version_unique_views",
            pa.int64(),
            "The total number of unique downloads for the version. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
        ),
        _field("version_title", pa.string(), "The name of the version in Zenodo."),
        _field(
            "version_id",
            pa.int64(),
            "The unique ID of the Zenodo version. This is identical to the version DOI.",
        ),
        _field(
            "version_record_id",
            pa.int64(),
            "The record ID of the Zenodo version. This is identical to the version ID.",
        ),
        _field(
            "concept_record_id",
            pa.int64(),
            "The concept record ID. This is shared between all versions of a record.",
        ),
        _field(
            "version_creation_date",
            pa.timestamp("s"),
            "The datetime the record was created.",
        ),
        _field(
            "version_last_modified_date",
            pa.timestamp("s"),
            "The datetime the record was last modified.",
        ),
        _field(
            "version_last_updated_date",
            pa.timestamp("s"),
            "The datetime the record was last updated.",
        ),
        _field(
            "version_publication_date",
            pa.timestamp("s"),
            "The date that the version was published.",
        ),
        _field("version_doi", pa.string(), "The DOI of the Zenodo version."),
        _field(
            "concept_record_doi",
            pa.string(),
            "The DOI of the Zenodo concept record.",
        ),
        _field("version_doi_url", pa.string(), "The DOI link of the Zenodo version."),
        _field("version_status", pa.string(), "The status of the Zenodo version."),
        _field("version_state", pa.string(), "The state of the Zenodo version."),
        _field("version_submitted", pa.bool_(), "Is the version submitted?"),
        _field(
            "version_description",
            pa.string(),
            "The description of the version.",
        ),
        _field("partition_key", pa.string()),
        _field(
            "software_hash_id",
            pa.string(),
            "A Software Heritage Software Hash ID (SWHID).",
        ),
    ],
    comment=(
        "Daily snapshot of download and view statistics for PUDL's archived "
        "Zenodo datasets and versions."
    ),
    primary_key=["version_id"],
)

core_eel_hole_log_ins = _table_schema(
    [
        _field(
            "insert_id",
            pa.string(),
            "A unique identifier for the log entry.",
        ),
        _field(
            "timestamp",
            pa.timestamp("s"),
            "The time the event described by the log entry occurred.",
        ),
        _field(
            "text_payload",
            pa.string(),
            "Data provided to the logger in a text format. For the viewer, this includes the redirects generated by a user when logging in.",
        ),
        _field(
            "log_in_query",
            pa.string(),
            "What was the user doing when they were motivated to log in? This mirrors query when the search prompted a log-in event.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment="Log-in events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_searches = _table_schema(
    [
        _field(
            "insert_id",
            pa.string(),
            "A unique identifier for the log entry.",
        ),
        _field(
            "user_id",
            pa.string(),
            "The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
        ),
        _field(
            "user_domain",
            pa.string(),
            "User's email domain - the part of a user's email address that follows the '@' symbol.",
        ),
        _field(
            "timestamp",
            pa.timestamp("s"),
            "The time the event described by the log entry occurred.",
        ),
        _field(
            "query",
            pa.string(),
            "What did a user type into the search box? This logs periodically, so multiple partial search queries may be logged as someone is typing.",
        ),
        _field("url", pa.string(), "What endpoint is a user hitting?"),
        _field(
            "session_id",
            pa.string(),
            "A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment="Search query events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_hits = _table_schema(
    [
        _field(
            "insert_id",
            pa.string(),
            "A unique identifier for the log entry.",
        ),
        _field(
            "timestamp",
            pa.timestamp("s"),
            "The time the event described by the log entry occurred.",
        ),
        _field(
            "name",
            pa.string(),
            "The name of the PUDL table returned by a search query. Only populated for 'hit' event types.",
        ),
        _field(
            "score",
            pa.float64(),
            "The table's relevance score based on the provided search query. Only populated for 'hit' event types.",
        ),
        _field(
            "tags",
            pa.string(),
            "The tags associated with a given table in the search results. Only populated for 'hit' events types.",
        ),
        _field("partition_key", pa.string()),
    ],
    comment="Search result hit events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

_eel_hole_preview_fields = [
    _field(
        "insert_id",
        pa.string(),
        "A unique identifier for the log entry.",
    ),
    _field(
        "user_id",
        pa.string(),
        "The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
    ),
    _field(
        "user_domain",
        pa.string(),
        "User's email domain - the part of a user's email address that follows the '@' symbol.",
    ),
    _field(
        "timestamp",
        pa.timestamp("s"),
        "The time the event described by the log entry occurred.",
    ),
    _field("url", pa.string(), "What endpoint is a user hitting?"),
    _field(
        "params_name", pa.string(), "The name of the table being queried using DuckDB."
    ),
    _field("params_page", pa.int64(), "The page of the query."),
    _field(
        "params_per_page",
        pa.int64(),
        "The number of records returned per DuckDB query. This is set by us, so it should be expected to hold constant without our intervention.",
    ),
]
"""Fields shared by core_eel_hole_previews and core_eel_hole_downloads."""

for _i in range(7):
    _suffix = "" if _i == 0 else f"_{_i}"
    _eel_hole_preview_fields.extend(
        [
            _field(
                f"params_filters_field_name{_suffix}",
                pa.string(),
                "The variable on which a user is performing a filter using DuckDB.",
            ),
            _field(
                f"params_filters_field_type{_suffix}",
                pa.string(),
                "The data type of the variable on which a user is performing a filter using DuckDB.",
            ),
            _field(
                f"params_filters_operation{_suffix}",
                pa.string(),
                "The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
            ),
            _field(
                f"params_filters_value{_suffix}",
                pa.string(),
                "The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
            ),
        ]
    )

_eel_hole_preview_fields.extend(
    [
        _field(
            "session_id",
            pa.string(),
            "A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
        ),
        _field("partition_key", pa.string()),
    ]
)

core_eel_hole_previews = _table_schema(
    _eel_hole_preview_fields,
    comment=(
        "DuckDB data preview query events recorded by PUDL's data viewer "
        "(the eel hole)."
    ),
    primary_key=["insert_id"],
)

core_eel_hole_downloads = _table_schema(
    _eel_hole_preview_fields,
    comment="Parquet download events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_user_settings_updates = _table_schema(
    [
        _field(
            "insert_id",
            pa.string(),
            "A unique identifier for the log entry.",
        ),
        _field(
            "user_id",
            pa.string(),
            "The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
        ),
        _field(
            "user_domain",
            pa.string(),
            "User's email domain - the part of a user's email address that follows the '@' symbol.",
        ),
        _field(
            "timestamp",
            pa.timestamp("s"),
            "The time the event described by the log entry occurred.",
        ),
        _field("accepted", pa.bool_(), "Has a user accepted the privacy policy?"),
        _field("newsletter", pa.bool_(), "Has a user subscribed to the newsletter?"),
        _field(
            "outreach",
            pa.bool_(),
            "Has a user agreed to be contacted for further discussion about PUDL?",
        ),
        _field("partition_key", pa.string()),
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
