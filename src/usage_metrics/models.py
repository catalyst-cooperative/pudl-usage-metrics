"""Pandera schemas for usage_metrics Parquet outputs.

Each table is defined once, as a :class:`pandera.pyarrow.DataFrameSchema`. This is the
source of truth for the table's column types, which columns may be null, its primary
key, and its documentation. The pyarrow schema and pandas dtypes used to write Parquet
files are derived from it by :mod:`usage_metrics.schemas`, and the asset checks in
:mod:`usage_metrics.checks` validate the written data with it directly.

Column descriptions and the table description are written into the Parquet file footer
(as field and schema metadata), so they travel with the data and can be read back with
e.g. ``pyarrow.parquet.read_schema(path).metadata``.

Pandera doesn't parse python's datetime.datetime so we need to use the native
:class:`pandera.dtypes.Timestamp`. Pandera also doesn't keep a timestamp's unit or time
zone, so timestamps are written to Parquet as naive microseconds.
"""

import collections
import copy

import pandera.pyarrow as pandera
from pandera.dtypes import Timestamp


def _column(name: str, dtype: type, comment: str | None = None) -> pandera.Column:
    """Build a nullable pandera Column with an optional description.

    Columns are nullable unless they are part of a primary key; see
    :func:`_table_schema`.
    """
    return pandera.Column(dtype, name=name, nullable=True, description=comment)


def _table_schema(
    name: str,
    columns: list[pandera.Column],
    comment: str,
    primary_key: list[str] | None = None,
) -> pandera.DataFrameSchema:
    """Build a pandera DataFrameSchema with a table description and primary key.

    The primary key columns must be non-null, and their values unique together.

    Raises:
        ValueError: if a column name is repeated, or a primary key column isn't one of
            the columns.
    """
    counts = collections.Counter(column.name for column in columns)
    if repeated := sorted(column for column, count in counts.items() if count > 1):
        raise ValueError(f"Table {name!r} repeats column names: {repeated}.")
    table_columns = {column.name: copy.deepcopy(column) for column in columns}
    primary_key = primary_key or []
    if missing := [key for key in primary_key if key not in table_columns]:
        raise ValueError(
            f"Table {name!r} has primary key columns {missing} that are not columns."
        )
    for col in primary_key:
        table_columns[col].nullable = False
    return pandera.DataFrameSchema(
        table_columns,
        name=name,
        unique=primary_key or None,
        strict=False,
        description=comment,
    )


# Metadata derived from:
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html#log-record-fields
# https://ipinfo.io/developers/lite-api
core_s3_logs = _table_schema(
    name="core_s3_logs",
    columns=[
        _column(name="id", dtype=str, comment="A unique ID for each log."),
        _column(
            name="time",
            dtype=Timestamp,
            comment="The time at which the request was received; these dates and times are in Coordinated Universal Time (UTC).",
        ),
        _column(
            name="request_uri",
            dtype=str,
            comment="The Request-URI part of the HTTP request message.",
        ),
        _column(
            name="operation",
            dtype=str,
            comment="The operation listed here is declared as SOAP.operation, REST.HTTP_method.resource_type, WEBSITE.HTTP_method.resource_type, or BATCH.DELETE.OBJECT, or S3.action.resource_type for S3 Lifecycle and logging. For Compute checksum job requests, the operation is listed as S3.COMPUTE.OBJECT.CHECKSUM.",
        ),
        _column(
            name="bucket",
            dtype=str,
            comment="The name of the bucket that the request was processed against. If the system receives a malformed request and cannot determine the bucket, the request will not appear in any server access log.",
        ),
        _column(
            name="bucket_owner",
            dtype=str,
            comment="The canonical user ID of the owner of the source bucket. The canonical user ID is another form of the AWS account ID.",
        ),
        _column(
            name="requester",
            dtype=str,
            comment="The canonical user ID of the requester, or null for unauthenticated requests. If the requester was an IAM user, this field returns the requester's IAM user name along with the AWS account that the IAM user belongs to. This identifier is the same one used for access control purposes.",
        ),
        _column(
            name="http_status",
            dtype=int,
            comment="The numeric HTTP status code of the response.",
        ),
        _column(
            name="megabytes_sent",
            dtype=float,
            comment="The total size of the object in question in megabytes.",
        ),
        _column(
            name="normalized_file_downloads",
            dtype=float,
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        # IP location
        _column(
            name="remote_ip",
            dtype=str,
            comment="The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _column(
            name="remote_ip_org",
            dtype=str,
            comment="IP Organization name, as determined by IPInfo.",
        ),
        _column(
            name="remote_ip_country_name",
            dtype=str,
            comment="Country where the IP is located, as determined by IPInfo.",
        ),
        _column(
            name="remote_ip_asn",
            dtype=str,
            comment="Autonomous System Number as determined by IPInfo.",
        ),
        _column(
            name="remote_ip_bogon",
            dtype=bool,
            comment="Is the IP address a bogon (bogus or invalid)?",
        ),
        _column(
            name="remote_ip_country",
            dtype=str,
            comment="ISO 3166 country code of the IP address, as determined by IPInfo.",
        ),
        # Other reported context
        _column(
            name="access_point_arn",
            dtype=str,
            comment="The Amazon Resource Name (ARN) of the access point of the request. If the access point ARN is malformed or not used, the field will be null",
        ),
        _column(
            name="acl_required",
            dtype=str,
            comment="A string that indicates whether the request required an access control list (ACL) for authorization. If the request required an ACL for authorization, the string is Yes. If no ACLs were required, the string is -.",
        ),
        _column(
            name="authentication_type",
            dtype=str,
            comment="The type of request authentication used: AuthHeader for authentication headers, QueryString for query string (presigned URL), or a - for unauthenticated requests.",
        ),
        _column(
            name="cipher_suite",
            dtype=str,
            comment="The Transport Layer Security (TLS) cipher that was negotiated for an HTTPS request or a - for HTTP.",
        ),
        _column(
            name="error_code",
            dtype=str,
            comment="The Amazon S3 Error responses of the GET portion of the copy operation, or - if no error occurred.",
        ),
        _column(
            name="host_header",
            dtype=str,
            comment="The endpoint that was used to connect to Amazon S3.",
        ),
        _column(
            name="host_id",
            dtype=str,
            comment="The x-amz-id-2 or Amazon S3 extended request ID.",
        ),
        _column(
            name="key",
            dtype=str,
            comment="The key (object name) of the object being copied, or - if the operation doesn't take a key parameter.",
        ),
        _column(
            name="object_size",
            dtype=float,
            comment="The total size of the object in question in bytes.",
        ),
        _column(
            name="request_id",
            dtype=str,
            comment="A string generated by Amazon S3 to uniquely identify each request. For Compute checksum job requests, the Request ID field displays the associated job ID.",
        ),
        _column(
            name="referer",
            dtype=str,
            comment="The value of the HTTP Referer header, if present. HTTP user-agents (for example, browsers) typically set this header to the URL of the linking or embedding page when making a request.",
        ),
        _column(
            name="signature_version",
            dtype=str,
            comment="The signature version, SigV2 or SigV4, that was used to authenticate the request, or a - for unauthenticated requests.",
        ),
        _column(
            name="tls_version",
            dtype=str,
            comment="The Transport Layer Security (TLS) version negotiated by the client. The value is one of following: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used.",
        ),
        _column(
            name="total_time",
            dtype=int,
            comment="The number of milliseconds that the request was in flight from the server's perspective. This value is measured from the time that your request is received to the time that the last byte of the response is sent. Measurements made from the client's perspective might be longer because of network latency.",
        ),
        _column(
            name="turn_around_time",
            dtype=float,
            comment="The number of milliseconds that Amazon S3 spent processing your request. This value is measured from the time that the last byte of your request was received until the time that the first byte of the response was sent.",
        ),
        _column(
            name="user_agent",
            dtype=str,
            comment="The value of the HTTP User-Agent header.",
        ),
        _column(
            name="version_id",
            dtype=str,
            comment="The version ID in the request, or - if the operation doesn't take a versionId parameter.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment="Cleaned per-request S3 access logs for PUDL's data distribution bucket.",
    primary_key=["id"],
)


out_s3_daily_summary_by_table = _table_schema(
    name="out_s3_daily_summary_by_table",
    columns=[
        _column(name="id", dtype=str, comment="A unique ID for each log."),
        _column(
            name="time",
            dtype=Timestamp,
            comment="The day for which metrics are reported.",
        ),
        _column(
            name="table",
            dtype=str,
            comment="The PUDL data table accessed by a user.",
        ),
        _column(
            name="version",
            dtype=str,
            comment="The version of the PUDL database (e.g., stable, nightly, 2026.1) accessed by a user.",
        ),
        _column(
            name="usage_type",
            dtype=str,
            comment="The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        _column(
            name="megabytes_sent",
            dtype=float,
            comment="The total size of the object in question in megabytes.",
        ),
        _column(
            name="normalized_file_downloads",
            dtype=float,
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        _column(
            name="request_count",
            dtype=int,
            comment="The number of requests made per table, usage method and day.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment="Daily S3 usage totals, aggregated by table and download/usage method.",
    primary_key=["id"],
)

out_s3_daily_summary_by_user = _table_schema(
    name="out_s3_daily_summary_by_user",
    columns=[
        _column(name="id", dtype=str, comment="A unique ID for each log."),
        _column(
            name="time",
            dtype=Timestamp,
            comment="The day for which metrics are reported.",
        ),
        _column(
            name="table",
            dtype=str,
            comment="The PUDL data table accessed by a user.",
        ),
        _column(
            name="version",
            dtype=str,
            comment="The version of the PUDL database (e.g., stable, nightly, 2026.1) accessed by a user.",
        ),
        _column(
            name="usage_type",
            dtype=str,
            comment="The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        # IP location
        _column(
            name="remote_ip",
            dtype=str,
            comment="The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
        ),
        _column(
            name="remote_ip_org",
            dtype=str,
            comment="IP Organization name, as determined by IPInfo.",
        ),
        _column(
            name="remote_ip_country_name",
            dtype=str,
            comment="Country where the IP is located, as determined by IPInfo.",
        ),
        _column(
            name="megabytes_sent",
            dtype=float,
            comment="The total size of the object in question in megabytes.",
        ),
        _column(
            name="normalized_file_downloads",
            dtype=float,
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        _column(
            name="request_count",
            dtype=int,
            comment="The number of requests made per table, usage method and day.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment="Daily S3 usage totals, aggregated by requester IP and download/usage method.",
    primary_key=["id"],
)

out_s3_daily_summary_by_db = _table_schema(
    name="out_s3_daily_summary_by_db",
    columns=[
        _column(name="id", dtype=str, comment="A unique ID for each log."),
        _column(
            name="time",
            dtype=Timestamp,
            comment="The day for which metrics are reported.",
        ),
        _column(
            name="database",
            dtype=str,
            comment="Which type of database the record is accessing (e.g., pudl.sqlite, ferc1.duckdb). Parquet files are lumped into one parquet_file record.",
        ),
        _column(name="version", dtype=str),
        _column(
            name="usage_type",
            dtype=str,
            comment="The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity.",
        ),
        _column(
            name="megabytes_sent",
            dtype=float,
            comment="The total size of the object in question in megabytes.",
        ),
        _column(
            name="normalized_file_downloads",
            dtype=float,
            comment="The proportion of the file that is downloaded (0 to 1).",
        ),
        _column(
            name="request_count",
            dtype=int,
            comment="The number of requests made per table, usage method and day.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment=(
        "Daily S3 usage totals, aggregated by database (e.g. pudl.sqlite, "
        "ferc1.duckdb) and download/usage method."
    ),
    primary_key=["id"],
)

core_kaggle_logs = _table_schema(
    name="core_kaggle_logs",
    columns=[
        _column(
            name="metrics_date",
            dtype=Timestamp,
            comment="The unique date for each metrics snapshot.",
        ),
        # Metrics on Kaggle usage
        _column(
            name="total_views",
            dtype=int,
            comment="How many people have viewed this dataset all-time.",
        ),
        _column(
            name="total_downloads",
            dtype=int,
            comment="How many people have downloaded this dataset all-time.",
        ),
        _column(
            name="total_votes",
            dtype=int,
            comment="How many people have upvoted this dataset all-time.",
        ),
        _column(
            name="usability_rating",
            dtype=float,
            comment="The current Kaggle usability rating (out of 10).",
        ),
        # Metadata on dataset
        _column(
            name="dataset_name",
            dtype=str,
            comment="The short-hand name (slug) of the dataset.",
        ),
        _column(name="owner", dtype=str, comment="The owner of the dataset."),
        _column(name="title", dtype=str, comment="The full title of the dataset."),
        _column(
            name="subtitle",
            dtype=str,
            comment="The subtitle of the dataset.",
        ),
        _column(
            name="description",
            dtype=str,
            comment="The description of the dataset.",
        ),
        _column(
            name="keywords",
            dtype=str,
            comment="All keywords associated with the dataset.",
        ),
        _column(
            name="expected_update_frequency",
            dtype=str,
            comment="How often the dataset is expected to be updated, as declared in its Kaggle metadata (e.g. 'not specified'). Only reported in newer data.",
        ),
        _column(
            name="dataset_id",
            dtype=str,
            comment="The unique dataset ID generated by Kaggle.",
        ),
        _column(
            name="is_private",
            dtype=str,
            comment="Whether the dataset is private (not viewable by the public).",
        ),
        _column(
            name="licenses",
            dtype=str,
            comment="A list of licenses attributed to the dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
        ),
        _column(
            name="collaborators",
            dtype=str,
            comment="A list of Kaggle users who are listed as collaborators on this dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
        ),
        _column(name="data", dtype=str),
        _column(name="partition_key", dtype=str),
    ],
    comment="Daily snapshot of PUDL's Kaggle dataset usage and metadata.",
    primary_key=["metrics_date"],
)

# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-sources
core_github_popular_referrers = _table_schema(
    name="core_github_popular_referrers",
    columns=[
        _column(
            name="metrics_date",
            dtype=Timestamp,
            comment="The date for each metrics snapshot.",
        ),
        _column(name="referrer", dtype=str, comment="The unique referrer."),
        _column(
            name="total_referrals",
            dtype=int,
            comment="Total number of referrals over the last 14 days.",
        ),
        _column(
            name="unique_referrals",
            dtype=int,
            comment="Unique number of referrals over the last 14 days.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment=(
        "Top 10 external referrers to the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date", "referrer"],
)

# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-paths
core_github_popular_paths = _table_schema(
    name="core_github_popular_paths",
    columns=[
        _column(
            name="metrics_date",
            dtype=Timestamp,
            comment="The date for each metrics snapshot.",
        ),
        _column(
            name="path",
            dtype=str,
            comment="One of the ten most popular Github paths on a given date.",
        ),
        _column(name="title", dtype=str, comment="Full title of the Github path."),
        _column(
            name="total_views",
            dtype=int,
            comment="Total views of the path over the last 14 days.",
        ),
        _column(
            name="unique_views",
            dtype=int,
            comment="Unique views of the path over the last 14 days.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment=(
        "Top 10 most-viewed paths in the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date", "path"],
)

# See https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones
core_github_clones = _table_schema(
    name="core_github_clones",
    columns=[
        _column(
            name="metrics_date",
            dtype=Timestamp,
            comment="The date for each metrics snapshot.",
        ),
        _column(
            name="total_clones",
            dtype=int,
            comment="Total number of clones of the PUDL repository over the last 14 days.",
        ),
        _column(
            name="unique_clones",
            dtype=int,
            comment="Unique number of clones of the PUDL repository over the last 14 days.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment=(
        "Daily count of clones of the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date"],
)

# See docs: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-page-views
core_github_views = _table_schema(
    name="core_github_views",
    columns=[
        _column(
            name="metrics_date",
            dtype=Timestamp,
            comment="The date for each metrics snapshot.",
        ),
        _column(
            name="total_views",
            dtype=int,
            comment="Total views of the repository over the last 14 days.",
        ),
        _column(
            name="unique_views",
            dtype=int,
            comment="Unique views of the repository over the last 14 days.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment=(
        "Daily count of views of the PUDL GitHub repository over a "
        "trailing 14-day window."
    ),
    primary_key=["metrics_date"],
)

# See docs: https://docs.github.com/en/rest/repos/forks
core_github_forks = _table_schema(
    name="core_github_forks",
    columns=[
        _column(
            name="id",
            dtype=int,
            comment="The unique identifier for each fork.",
        ),
        _column(
            name="node_id",
            dtype=str,
            comment="The global node ID of the fork in Github.",
        ),
        _column(name="name", dtype=str, comment="Name of fork."),
        _column(
            name="full_name",
            dtype=str,
            comment="Full name of fork, including repository.",
        ),
        _column(name="private", dtype=bool, comment="Is this fork private?"),
        _column(name="owner", dtype=str, comment="Metadata about the owner."),
        _column(name="description", dtype=str, comment="Description of the fork."),
        _column(
            name="url",
            dtype=str,
            comment="API link to the forked repoitory.",
        ),
        _column(
            name="created_at",
            dtype=Timestamp,
            comment="Time the repository was created, in UTC.",
        ),
        _column(
            name="updated_at",
            dtype=Timestamp,
            comment="Time the repository was last updated, in UTC.",
        ),
        _column(
            name="pushed_at",
            dtype=Timestamp,
            comment="Time of the last pushed commit, in UTC.",
        ),
        _column(
            name="homepage",
            dtype=str,
            comment="Home page of the repository.",
        ),
        _column(name="size_kb", dtype=int, comment="Size in KB of the repository."),
        _column(
            name="stargazers_count",
            dtype=int,
            comment="Count of how many people have starred the repository.",
        ),
        _column(
            name="watchers_count",
            dtype=int,
            comment="Count of how many people are watching the repository.",
        ),
        _column(name="language", dtype=str, comment="Repository language."),
        _column(
            name="has_issues",
            dtype=bool,
            comment="Does the repository have issues?",
        ),
        _column(
            name="has_projects",
            dtype=bool,
            comment="Does the repository have projects?",
        ),
        _column(
            name="has_downloads",
            dtype=bool,
            comment="Does the repository have downloads?",
        ),
        _column(
            name="has_wiki",
            dtype=bool,
            comment="Does the repository have a wiki?",
        ),
        _column(
            name="has_pages",
            dtype=bool,
            comment="Does the repository have pages?",
        ),
        _column(
            name="has_discussions",
            dtype=bool,
            comment="Does the repository have discussions?",
        ),
        _column(
            name="has_pull_requests",
            dtype=bool,
            comment="Does the repository have pull requests?",
        ),
        _column(
            name="forks_count",
            dtype=int,
            comment="Count of forks of the forked repository.",
        ),
        _column(name="archived", dtype=bool, comment="Is this repository archived?"),
        _column(name="disabled", dtype=bool, comment="Is this repository disabled?"),
        _column(name="license", dtype=str, comment="License of the repository."),
        _column(
            name="allow_forking",
            dtype=bool,
            comment="Does the repository allow forking?",
        ),
        _column(
            name="is_template",
            dtype=bool,
            comment="Is the repository a template?",
        ),
        _column(
            name="web_commit_signoff_required",
            dtype=bool,
            comment="Does the repository require signoffs for web-based commits?",
        ),
        _column(
            name="topics",
            dtype=str,
            comment="A list of topics associated with the repository.",
        ),
        _column(
            name="visibility",
            dtype=str,
            comment="The visibility setting of the repository.",
        ),
        _column(
            name="forks",
            dtype=int,
            comment="How many forks are there for this repository?",
        ),
        _column(
            name="open_issues",
            dtype=int,
            comment="How many open issues are there in this repository?",
        ),
        _column(
            name="watchers",
            dtype=int,
            comment="How many people are watching this repository?",
        ),
        _column(
            name="default_branch",
            dtype=str,
            comment="The default branch of the repository.",
        ),
        _column(
            name="permissions",
            dtype=str,
            comment="Permissions settings on the repository.",
        ),
    ],
    comment="Snapshot of all forks of the PUDL GitHub repository.",
    primary_key=["id"],
)

# See docs: https://docs.github.com/en/rest/activity/starring
core_github_stargazers = _table_schema(
    name="core_github_stargazers",
    columns=[
        _column(
            name="id",
            dtype=int,
            comment="The unique identifier for each stargazer.",
        ),
        _column(
            name="starred_at",
            dtype=Timestamp,
            comment="When the user starred the repository, in UTC.",
        ),
        _column(name="login", dtype=str, comment="Github username."),
        _column(
            name="node_id",
            dtype=str,
            comment="The global node ID of the fork in Github.",
        ),
        _column(name="url", dtype=str, comment="API link to the user account."),
        _column(
            name="html_url",
            dtype=str,
            comment="HTML link to the user account.",
        ),
        _column(
            name="followers_url",
            dtype=str,
            comment="API link to the user's followers.",
        ),
        _column(
            name="following_url",
            dtype=str,
            comment="API link to a list of users that the user is following.",
        ),
        _column(
            name="gists_url",
            dtype=str,
            comment="API link to a list of the user's gists.",
        ),
        _column(
            name="starred_url",
            dtype=str,
            comment="API link to a list of the user's starred repositories.",
        ),
        _column(
            name="subscriptions_url",
            dtype=str,
            comment="API link to a list of the user's subscriptions.",
        ),
        _column(
            name="organizations_url",
            dtype=str,
            comment="API link to a list of the user's organizations.",
        ),
        _column(
            name="repos_url",
            dtype=str,
            comment="API link to a list of the user's repositories.",
        ),
        _column(
            name="events_url",
            dtype=str,
            comment="API link to a list of the user's events.",
        ),
        _column(
            name="received_events_url",
            dtype=str,
            comment="API link to a list of the user's received events.",
        ),
        _column(name="type", dtype=str, comment="Type of entity (e.g., user)."),
        _column(name="site_admin", dtype=bool, comment="Is this user a site admin?"),
    ],
    comment="Snapshot of all users who have starred the PUDL GitHub repository.",
    primary_key=["id"],
)

# See: https://zenodo.org/help/statistics
core_zenodo_logs = _table_schema(
    name="core_zenodo_logs",
    columns=[
        _column(
            name="metrics_date",
            dtype=Timestamp,
            comment="The date when the metadata was reported.",
        ),
        _column(
            name="version",
            dtype=str,
            comment="The version (e.g. 10.0.0) of the dataset record.",
        ),
        _column(
            name="dataset_slug",
            dtype=str,
            comment="The shorthand for the dataset being archived. Matches the pudl_archiver repository dataset slugs when the dataset is archived by the PUDL archiver.",
        ),
        _column(
            name="dataset_downloads",
            dtype=int,
            comment="The total number of downloads for the entire dataset. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
        ),
        _column(
            name="dataset_unique_downloads",
            dtype=int,
            comment="The total number of unique downloads for the entire dataset. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
        ),
        _column(
            name="dataset_views",
            dtype=int,
            comment="The total number of views for the entire dataset. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
        ),
        _column(
            name="dataset_unique_views",
            dtype=int,
            comment="The total number of unique downloads for the entire dataset. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
        ),
        _column(
            name="version_downloads",
            dtype=int,
            comment="The total number of downloads for the version. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
        ),
        _column(
            name="version_unique_downloads",
            dtype=int,
            comment="The total number of unique downloads for the version. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
        ),
        _column(
            name="version_views",
            dtype=int,
            comment="The total number of views for the version. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
        ),
        _column(
            name="version_unique_views",
            dtype=int,
            comment="The total number of unique downloads for the version. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
        ),
        _column(
            name="version_title",
            dtype=str,
            comment="The name of the version in Zenodo.",
        ),
        _column(
            name="version_id",
            dtype=int,
            comment="The unique ID of the Zenodo version. This is identical to the version DOI.",
        ),
        _column(
            name="version_record_id",
            dtype=int,
            comment="The record ID of the Zenodo version. This is identical to the version ID.",
        ),
        _column(
            name="concept_record_id",
            dtype=int,
            comment="The concept record ID. This is shared between all versions of a record.",
        ),
        _column(
            name="version_creation_date",
            dtype=Timestamp,
            comment="The datetime the record was created.",
        ),
        _column(
            name="version_last_modified_date",
            dtype=Timestamp,
            comment="The datetime the record was last modified.",
        ),
        _column(
            name="version_last_updated_date",
            dtype=Timestamp,
            comment="The datetime the record was last updated.",
        ),
        _column(
            name="version_publication_date",
            dtype=Timestamp,
            comment="The date that the version was published.",
        ),
        _column(
            name="version_doi",
            dtype=str,
            comment="The DOI of the Zenodo version.",
        ),
        _column(
            name="concept_record_doi",
            dtype=str,
            comment="The DOI of the Zenodo concept record.",
        ),
        _column(
            name="version_doi_url",
            dtype=str,
            comment="The DOI link of the Zenodo version.",
        ),
        _column(
            name="version_status",
            dtype=str,
            comment="The status of the Zenodo version.",
        ),
        _column(
            name="version_state",
            dtype=str,
            comment="The state of the Zenodo version.",
        ),
        _column(
            name="version_submitted",
            dtype=bool,
            comment="Is the version submitted?",
        ),
        _column(
            name="version_description",
            dtype=str,
            comment="The description of the version.",
        ),
        _column(name="partition_key", dtype=str),
        _column(
            name="software_hash_id",
            dtype=str,
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
    name="core_eel_hole_log_ins",
    columns=[
        _column(
            name="insert_id",
            dtype=str,
            comment="A unique identifier for the log entry.",
        ),
        _column(
            name="timestamp",
            dtype=Timestamp,
            comment="The time the event described by the log entry occurred.",
        ),
        _column(
            name="text_payload",
            dtype=str,
            comment="Data provided to the logger in a text format. For the viewer, this includes the redirects generated by a user when logging in.",
        ),
        _column(
            name="log_in_query",
            dtype=str,
            comment="What was the user doing when they were motivated to log in? This mirrors query when the search prompted a log-in event.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment="Log-in events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_searches = _table_schema(
    name="core_eel_hole_searches",
    columns=[
        _column(
            name="insert_id",
            dtype=str,
            comment="A unique identifier for the log entry.",
        ),
        _column(
            name="user_id",
            dtype=str,
            comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
        ),
        _column(
            name="user_domain",
            dtype=str,
            comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
        ),
        _column(
            name="timestamp",
            dtype=Timestamp,
            comment="The time the event described by the log entry occurred.",
        ),
        _column(
            name="query",
            dtype=str,
            comment="What did a user type into the search box? This logs periodically, so multiple partial search queries may be logged as someone is typing.",
        ),
        _column(name="url", dtype=str, comment="What endpoint is a user hitting?"),
        _column(
            name="session_id",
            dtype=str,
            comment="A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment="Search query events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_hits = _table_schema(
    name="core_eel_hole_hits",
    columns=[
        _column(
            name="insert_id",
            dtype=str,
            comment="A unique identifier for the log entry.",
        ),
        _column(
            name="timestamp",
            dtype=Timestamp,
            comment="The time the event described by the log entry occurred.",
        ),
        _column(
            name="name",
            dtype=str,
            comment="The name of the PUDL table returned by a search query. Only populated for 'hit' event types.",
        ),
        _column(
            name="score",
            dtype=float,
            comment="The table's relevance score based on the provided search query. Only populated for 'hit' event types.",
        ),
        _column(
            name="tags",
            dtype=str,
            comment="The tags associated with a given table in the search results. Only populated for 'hit' events types.",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment="Search result hit events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)


def _eel_hole_filter_columns(suffix: str) -> list[pandera.Column]:
    """Columns describing one DuckDB filter in an eel hole preview/download query.

    The viewer logs up to seven filters per query, with the columns of the first
    unsuffixed and the rest suffixed ``_1`` through ``_6``.
    """
    return [
        _column(
            name=f"params_filters_field_name{suffix}",
            dtype=str,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        _column(
            name=f"params_filters_field_type{suffix}",
            dtype=str,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        _column(
            name=f"params_filters_operation{suffix}",
            dtype=str,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        _column(
            name=f"params_filters_value{suffix}",
            dtype=str,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
    ]


_eel_hole_preview_columns = [
    _column(
        name="insert_id",
        dtype=str,
        comment="A unique identifier for the log entry.",
    ),
    _column(
        name="user_id",
        dtype=str,
        comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
    ),
    _column(
        name="user_domain",
        dtype=str,
        comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
    ),
    _column(
        name="timestamp",
        dtype=Timestamp,
        comment="The time the event described by the log entry occurred.",
    ),
    _column(name="url", dtype=str, comment="What endpoint is a user hitting?"),
    _column(
        name="params_name",
        dtype=str,
        comment="The name of the table being queried using DuckDB.",
    ),
    _column(name="params_page", dtype=int, comment="The page of the query."),
    _column(
        name="params_per_page",
        dtype=int,
        comment="The number of records returned per DuckDB query. This is set by us, so it should be expected to hold constant without our intervention.",
    ),
    *(
        column
        for i in range(7)
        for column in _eel_hole_filter_columns(suffix="" if i == 0 else f"_{i}")
    ),
    _column(
        name="session_id",
        dtype=str,
        comment="A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
    ),
    _column(name="partition_key", dtype=str),
]
"""Columns shared by core_eel_hole_previews and core_eel_hole_downloads.

Rather than specifying these columns twice, we define them once and use the list for
both tables. This avoids duplication and keeps the two tables consistent.
"""

core_eel_hole_previews = _table_schema(
    name="core_eel_hole_previews",
    columns=_eel_hole_preview_columns,
    comment=(
        "DuckDB data preview query events recorded by PUDL's data viewer "
        "(the eel hole)."
    ),
    primary_key=["insert_id"],
)

core_eel_hole_downloads = _table_schema(
    name="core_eel_hole_downloads",
    columns=_eel_hole_preview_columns,
    comment="Parquet download events recorded by PUDL's data viewer (the eel hole).",
    primary_key=["insert_id"],
)

core_eel_hole_user_settings_updates = _table_schema(
    name="core_eel_hole_user_settings_updates",
    columns=[
        _column(
            name="insert_id",
            dtype=str,
            comment="A unique identifier for the log entry.",
        ),
        _column(
            name="user_id",
            dtype=str,
            comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
        ),
        _column(
            name="user_domain",
            dtype=str,
            comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
        ),
        _column(
            name="timestamp",
            dtype=Timestamp,
            comment="The time the event described by the log entry occurred.",
        ),
        _column(
            name="accepted",
            dtype=bool,
            comment="Has a user accepted the privacy policy?",
        ),
        _column(
            name="newsletter",
            dtype=bool,
            comment="Has a user subscribed to the newsletter?",
        ),
        _column(
            name="outreach",
            dtype=bool,
            comment="Has a user agreed to be contacted for further discussion about PUDL?",
        ),
        _column(name="partition_key", dtype=str),
    ],
    comment=(
        "User privacy/newsletter/outreach setting updates recorded by PUDL's "
        "data viewer (the eel hole)."
    ),
    primary_key=["insert_id"],
)

usage_metrics_schemas: dict[str, pandera.DataFrameSchema] = {
    schema.name: schema
    for schema in [
        core_s3_logs,
        out_s3_daily_summary_by_table,
        out_s3_daily_summary_by_user,
        out_s3_daily_summary_by_db,
        core_kaggle_logs,
        core_github_popular_referrers,
        core_github_popular_paths,
        core_github_clones,
        core_github_views,
        core_github_forks,
        core_github_stargazers,
        core_zenodo_logs,
        core_eel_hole_log_ins,
        core_eel_hole_searches,
        core_eel_hole_hits,
        core_eel_hole_previews,
        core_eel_hole_downloads,
        core_eel_hole_user_settings_updates,
    ]
}
