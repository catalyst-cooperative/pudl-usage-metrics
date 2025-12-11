"""SQLAlchemy models for usage_metrics database."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)

usage_metrics_metadata = MetaData()

datasette_request_logs = Table(
    "datasette_request_logs",
    usage_metrics_metadata,
    Column("insert_id", String, primary_key=True, comment="A unique ID for each log."),
    Column("log_name", String),
    Column("resource", String),
    Column("text_payload", String),
    Column("timestamp", DateTime),
    Column("receive_timestamp", DateTime),
    Column("severity", String),
    Column("http_request", String),
    Column("labels", String),
    Column("operation", String),
    Column("trace", String),
    Column("span_id", String),
    Column("trace_sampled", Boolean),
    Column("source_location", String),
    Column("cache_hit", String),
    Column("cache_lookup", String),
    Column("request_url", String),
    Column("protocol", String),
    Column("cache_fill_bytes", String),
    Column("response_size", Float),
    Column("server_ip", String),
    Column("cache_validated_with_origin_server", String),
    Column("request_method", String),
    Column("request_size", Integer),
    Column("user_agent", String),
    Column("status", Integer),
    Column("referer", String),
    Column("latency", Float),
    Column("remote_ip", String),
    Column("request_url_path", String),
    Column("request_url_query", String),
    Column("request_url_scheme", String),
    Column("request_url_netloc", String),
    Column("remote_ip_city", String),
    Column("remote_ip_loc", String),
    Column("remote_ip_org", String),
    Column("remote_ip_hostname", String),
    Column("remote_ip_country_name", String),
    Column("remote_ip_asn", String),
    Column("remote_ip_country", String),
    Column("remote_ip_timezone", String),
    Column("remote_ip_latitude", Float),
    Column("remote_ip_longitude", Float),
    Column("remote_ip_postal", String),
    Column("remote_ip_region", String),
    Column("remote_ip_full_location", String),
    Column("partition_key", String),
)

# Metadata derived from:
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html#log-record-fields
# https://ipinfo.io/developers/lite-api
core_s3_logs = Table(
    "core_s3_logs",
    usage_metrics_metadata,
    Column("id", String, primary_key=True, comment="A unique ID for each log."),
    # Query information
    Column(
        "time",
        DateTime,
        comment="The time at which the request was received; these dates and times are in Coordinated Universal Time (UTC).",
    ),
    Column(
        "request_uri",
        String,
        comment="The Request-URI part of the HTTP request message.",
    ),
    Column(
        "operation",
        String,
        comment="The operation listed here is declared as SOAP.operation, REST.HTTP_method.resource_type, WEBSITE.HTTP_method.resource_type, or BATCH.DELETE.OBJECT, or S3.action.resource_type for S3 Lifecycle and logging. For Compute checksum job requests, the operation is listed as S3.COMPUTE.OBJECT.CHECKSUM.",
    ),
    Column(
        "bucket",
        String,
        comment="The name of the bucket that the request was processed against. If the system receives a malformed request and cannot determine the bucket, the request will not appear in any server access log.",
    ),
    Column(
        "bucket_owner",
        String,
        comment="The canonical user ID of the owner of the source bucket. The canonical user ID is another form of the AWS account ID.",
    ),
    Column(
        "requester",
        String,
        comment="The canonical user ID of the requester, or null for unauthenticated requests. If the requester was an IAM user, this field returns the requester's IAM user name along with the AWS account that the IAM user belongs to. This identifier is the same one used for access control purposes.",
    ),
    Column(
        "http_status", Integer, comment="The numeric HTTP status code of the response."
    ),
    Column(
        "megabytes_sent",
        Float,
        comment="The total size of the object in question in megabytes.",
    ),
    Column(
        "normalized_file_downloads",
        Float,
        comment="The proportion of the file that is downloaded (0 to 1).",
    ),
    # IP location
    Column(
        "remote_ip",
        String,
        comment="The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
    ),
    Column(
        "remote_ip_city",
        String,
        comment="City where the IP is located, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_loc",
        String,
        comment="Geospatial coordinates of the IP, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_org",
        String,
        comment="IP Organization name, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_hostname",
        String,
        comment="Name of the IP host, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_country_name",
        String,
        comment="Country where the IP is located, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_asn",
        String,
        comment="Autonomous System Number as determined by IPInfo.",
    ),
    Column(
        "remote_ip_bogon",
        Boolean,
        comment="Is the IP address a bogon (bogus or invalid)?",
    ),
    Column(
        "remote_ip_country",
        String,
        comment="ISO 3166 country code of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_timezone",
        String,
        comment="Timezone of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_latitude",
        Float,
        comment="Latitude of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_longitude",
        Float,
        comment="Longitude of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_postal",
        String,
        comment="Postcode or zipcode of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_region",
        String,
        comment="Region/state of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_full_location",
        String,
        comment="Full address of the IP address, as determined by IPInfo.",
    ),
    # Other reported context
    Column(
        "access_point_arn",
        String,
        comment="The Amazon Resource Name (ARN) of the access point of the request. If the access point ARN is malformed or not used, the field will be null",
    ),
    Column(
        "acl_required",
        String,
        comment="A string that indicates whether the request required an access control list (ACL) for authorization. If the request required an ACL for authorization, the string is Yes. If no ACLs were required, the string is -.",
    ),
    Column(
        "authentication_type",
        String,
        comment="The type of request authentication used: AuthHeader for authentication headers, QueryString for query string (presigned URL), or a - for unauthenticated requests.",
    ),
    Column(
        "cipher_suite",
        String,
        comment="The Transport Layer Security (TLS) cipher that was negotiated for an HTTPS request or a - for HTTP.",
    ),
    Column(
        "error_code",
        String,
        comment="The Amazon S3 Error responses of the GET portion of the copy operation, or - if no error occurred.",
    ),
    Column(
        "host_header",
        String,
        comment="The endpoint that was used to connect to Amazon S3.",
    ),
    Column(
        "host_id", String, comment="The x-amz-id-2 or Amazon S3 extended request ID."
    ),
    Column(
        "key",
        String,
        comment="The key (object name) of the object being copied, or - if the operation doesn't take a key parameter.",
    ),
    Column(
        "object_size",
        Float,
        comment="The total size of the object in question in bytes.",
    ),
    Column(
        "request_id",
        String,
        comment="A string generated by Amazon S3 to uniquely identify each request. For Compute checksum job requests, the Request ID field displays the associated job ID.",
    ),
    Column(
        "referer",
        String,
        comment="The value of the HTTP Referer header, if present. HTTP user-agents (for example, browsers) typically set this header to the URL of the linking or embedding page when making a request.",
    ),
    Column(
        "signature_version",
        String,
        comment="The signature version, SigV2 or SigV4, that was used to authenticate the request, or a - for unauthenticated requests.",
    ),
    Column(
        "tls_version",
        String,
        comment="The Transport Layer Security (TLS) version negotiated by the client. The value is one of following: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used.",
    ),
    Column(
        "total_time",
        BigInteger,
        comment="The number of milliseconds that the request was in flight from the server's perspective. This value is measured from the time that your request is received to the time that the last byte of the response is sent. Measurements made from the client's perspective might be longer because of network latency.",
    ),
    Column(
        "turn_around_time",
        Float,
        comment="The number of milliseconds that Amazon S3 spent processing your request. This value is measured from the time that the last byte of your request was received until the time that the first byte of the response was sent.",
    ),
    Column("user_agent", String, comment="The value of the HTTP User-Agent header."),
    Column(
        "version_id",
        String,
        comment="The version ID in the request, or - if the operation doesn't take a versionId parameter.",
    ),
    Column("partition_key", String),
)

out_s3_logs = Table(
    "out_s3_logs",
    usage_metrics_metadata,
    Column("id", String, primary_key=True, comment="A unique ID for each log."),
    # Query information
    Column(
        "time",
        DateTime,
        comment="The time at which the request was received; these dates and times are in Coordinated Universal Time (UTC).",
    ),
    Column("table", String),
    Column("version", String),
    Column(
        "usage_type",
        String,
        comment=(
            "The type of usage activity. Distinguishes between requests made through DuckDB via the eel hole (eel_hole_duckdb), by clicking the download Parquet button in the eel hole (eel_hole_link), by clicking a download link from the docs, or other direct S3 activity."
        ),
    ),
    # IP location
    Column(
        "remote_ip",
        String,
        comment="The apparent IP address of the requester. Intermediate proxies and firewalls might obscure the actual IP address of the machine that's making the request.",
    ),
    Column(
        "remote_ip_city",
        String,
        comment="City where the IP is located, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_loc",
        String,
        comment="Geospatial coordinates of the IP, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_org",
        String,
        comment="IP Organization name, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_hostname",
        String,
        comment="Name of the IP host, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_country_name",
        String,
        comment="Country where the IP is located, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_asn",
        String,
        comment="Autonomous System Number as determined by IPInfo.",
    ),
    Column(
        "remote_ip_bogon",
        Boolean,
        comment="Is the IP address a bogon (bogus or invalid)?",
    ),
    Column(
        "remote_ip_country",
        String,
        comment="ISO 3166 country code of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_timezone",
        String,
        comment="Timezone of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_latitude",
        Float,
        comment="Latitude of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_longitude",
        Float,
        comment="Longitude of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_postal",
        String,
        comment="Postcode or zipcode of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_region",
        String,
        comment="Region/state of the IP address, as determined by IPInfo.",
    ),
    Column(
        "remote_ip_full_location",
        String,
        comment="Full address of the IP address, as determined by IPInfo.",
    ),
    # Other reported context
    Column(
        "access_point_arn",
        String,
        comment="The Amazon Resource Name (ARN) of the access point of the request. If the access point ARN is malformed or not used, the field will be null",
    ),
    Column(
        "acl_required",
        String,
        comment="A string that indicates whether the request required an access control list (ACL) for authorization. If the request required an ACL for authorization, the string is Yes. If no ACLs were required, the string is -.",
    ),
    Column(
        "authentication_type",
        String,
        comment="The type of request authentication used: AuthHeader for authentication headers, QueryString for query string (presigned URL), or a - for unauthenticated requests.",
    ),
    Column(
        "megabytes_sent",
        Float,
        comment="The total size of the object in question in megabytes.",
    ),
    Column(
        "normalized_file_downloads",
        Float,
        comment="The proportion of the file that is downloaded (0 to 1).",
    ),
    Column(
        "cipher_suite",
        String,
        comment="The Transport Layer Security (TLS) cipher that was negotiated for an HTTPS request or a - for HTTP.",
    ),
    Column(
        "error_code",
        String,
        comment="The Amazon S3 Error responses of the GET portion of the copy operation, or - if no error occurred.",
    ),
    Column(
        "host_header",
        String,
        comment="The endpoint that was used to connect to Amazon S3.",
    ),
    Column(
        "host_id", String, comment="The x-amz-id-2 or Amazon S3 extended request ID."
    ),
    Column(
        "http_status", Integer, comment="The numeric HTTP status code of the response."
    ),
    Column(
        "key",
        String,
        comment="The key (object name) of the object being copied, or - if the operation doesn't take a key parameter.",
    ),
    Column(
        "object_size",
        Float,
        comment="The total size of the object in question in bytes.",
    ),
    Column(
        "referer",
        String,
        comment="The value of the HTTP Referer header, if present. HTTP user-agents (for example, browsers) typically set this header to the URL of the linking or embedding page when making a request.",
    ),
    Column(
        "request_id",
        String,
        comment="A string generated by Amazon S3 to uniquely identify each request. For Compute checksum job requests, the Request ID field displays the associated job ID.",
    ),
    Column(
        "request_uri",
        String,
        comment="The Request-URI part of the HTTP request message.",
    ),
    Column(
        "signature_version",
        String,
        comment="The signature version, SigV2 or SigV4, that was used to authenticate the request, or a - for unauthenticated requests.",
    ),
    Column(
        "tls_version",
        String,
        comment="The Transport Layer Security (TLS) version negotiated by the client. The value is one of following: TLSv1.1, TLSv1.2, TLSv1.3, or - if TLS wasn't used.",
    ),
    Column(
        "total_time",
        BigInteger,
        comment="The number of milliseconds that the request was in flight from the server's perspective. This value is measured from the time that your request is received to the time that the last byte of the response is sent. Measurements made from the client's perspective might be longer because of network latency.",
    ),
    Column(
        "turn_around_time",
        Float,
        comment="The number of milliseconds that Amazon S3 spent processing your request. This value is measured from the time that the last byte of your request was received until the time that the first byte of the response was sent.",
    ),
    Column("user_agent", String, comment="The value of the HTTP User-Agent header."),
    Column(
        "version_id",
        String,
        comment="The version ID in the request, or - if the operation doesn't take a versionId parameter.",
    ),
    Column("partition_key", String),
)

core_kaggle_logs = Table(
    "core_kaggle_logs",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        DateTime,
        primary_key=True,
        comment="The unique date for each metrics snapshot.",
    ),
    # Metrics on Kaggle usage
    Column(
        "total_views",
        Integer,
        comment="How many people have viewed this dataset all-time.",
    ),
    Column(
        "total_downloads",
        Integer,
        comment="How many people have downloaded this dataset all-time.",
    ),
    Column(
        "total_votes",
        Integer,
        comment="How many people have upvoted this dataset all-time.",
    ),
    Column(
        "usability_rating",
        Float,
        comment="The current Kaggle usability rating (out of 10).",
    ),
    # Metadata on dataset
    Column(
        "dataset_name", String, comment="The short-hand name (slug) of the dataset."
    ),
    Column("owner", String, comment="The owner of the dataset."),
    Column("title", String, comment="The full title of the dataset."),
    Column("subtitle", String, comment="The subtitle of the dataset."),
    Column("description", String, comment="The description of the dataset."),
    Column("keywords", String, comment="All keywords associated with the dataset."),
    Column("dataset_id", String, comment="The unique dataset ID generated by Kaggle."),
    Column(
        "is_private",
        String,
        comment="Whether the dataset is private (not viewable by the public).",
    ),
    Column(
        "licenses",
        String,
        comment="A list of licenses attributed to the dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
    ),
    Column(
        "collaborators",
        String,
        comment="A list of Kaggle users who are listed as collaborators on this dataset. This is a list of dictionaries that has been dumped into a string during processing as it has no analytical value.",
    ),
    Column("data", String),
    Column("partition_key", String),
)
# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-sources
core_github_popular_referrers = Table(
    "core_github_popular_referrers",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        Date,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column("referrer", String, primary_key=True, comment="The unique referrer."),
    Column(
        "total_referrals",
        Integer,
        comment="Total number of referrals over the last 14 days.",
    ),
    Column(
        "unique_referrals",
        Integer,
        comment="Unique number of referrals over the last 14 days.",
    ),
    Column("partition_key", String),
)

# See: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-top-referral-paths
core_github_popular_paths = Table(
    "core_github_popular_paths",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        Date,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column(
        "path",
        String,
        primary_key=True,
        comment="One of the ten most popular Github paths on a given date.",
    ),
    Column("title", String, comment="Full title of the Github path."),
    Column(
        "total_views", Integer, comment="Total views of the path over the last 14 days."
    ),
    Column(
        "unique_views",
        Integer,
        comment="Unique views of the path over the last 14 days.",
    ),
    Column("partition_key", String),
)

# See https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-repository-clones
core_github_clones = Table(
    "core_github_clones",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        Date,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column(
        "total_clones",
        Integer,
        comment="Total number of clones of the PUDL repository over the last 14 days.",
    ),
    Column(
        "unique_clones",
        Integer,
        comment="Unique number of clones of the PUDL repository over the last 14 days.",
    ),
    Column("partition_key", String),
)

# See docs: https://docs.github.com/en/rest/metrics/traffic?apiVersion=2022-11-28#get-page-views
core_github_views = Table(
    "core_github_views",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        Date,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column(
        "total_views",
        Integer,
        comment="Total views of the repository over the last 14 days.",
    ),
    Column(
        "unique_views",
        Integer,
        comment="Unique views of the repository over the last 14 days.",
    ),
    Column("partition_key", String),
)

# See docs: https://docs.github.com/en/rest/repos/forks
core_github_forks = Table(
    "core_github_forks",
    usage_metrics_metadata,
    Column(
        "id", Integer, primary_key=True, comment="The unique identifier for each fork."
    ),
    Column("node_id", String, comment="The global node ID of the fork in Github."),
    Column("name", String, comment="Name of fork."),
    Column("full_name", String, comment="Full name of fork, including repository."),
    Column("private", Boolean, comment="Is this fork private?"),
    Column("owner", String, comment="Metadata about the owner."),
    Column("description", String, comment="Description of the fork."),
    Column("url", String, comment="API link to the forked repoitory."),
    Column("created_at", DateTime, comment="Time the repository was created, in UTC."),
    Column(
        "updated_at", DateTime, comment="Time the repository was last updated, in UTC."
    ),
    Column("pushed_at", DateTime, comment="Time of the last pushed commit, in UTC."),
    Column("homepage", String, comment="Home page of the repository."),
    Column("size_kb", Integer, comment="Size in KB of the repository."),
    Column(
        "stargazers_count",
        Integer,
        comment="Count of how many people have starred the repository.",
    ),
    Column(
        "watchers_count",
        Integer,
        comment="Count of how many people are watching the repository.",
    ),
    Column("language", String, comment="Repository language."),
    Column("has_issues", Boolean, comment="Does the repository have issues?"),
    Column("has_projects", Boolean, comment="Does the repository have projects?"),
    Column("has_downloads", Boolean, comment="Does the repository have downloads?"),
    Column("has_wiki", Boolean, comment="Does the repository have a wiki?"),
    Column("has_pages", Boolean, comment="Does the repository have pages?"),
    Column("has_discussions", Boolean, comment="Does the repository have discussions?"),
    Column("forks_count", Integer, comment="Count of forks of the forked repository."),
    Column("archived", Boolean, comment="Is this repository archived?"),
    Column("disabled", Boolean, comment="Is this repository disabled?"),
    Column("license", String, comment="License of the repository."),
    Column("allow_forking", Boolean, comment="Does the repository allow forking?"),
    Column("is_template", Boolean, comment="Is the repository a template?"),
    Column(
        "web_commit_signoff_required",
        Boolean,
        comment="Does the repository require signoffs for web-based commits?",
    ),
    Column(
        "topics", String, comment="A list of topics associated with the repository."
    ),
    Column("visibility", String, comment="The visibility setting of the repository."),
    Column("forks", Integer, comment="How many forks are there for this repository?"),
    Column(
        "open_issues",
        Integer,
        comment="How many open issues are there in this repository?",
    ),
    Column(
        "watchers", Integer, comment="How many people are watching this repository?"
    ),
    Column("default_branch", String, comment="The default branch of the repository."),
    Column("permissions", String, comment="Permissions settings on the repository."),
)

# See docs: https://docs.github.com/en/rest/activity/starring
core_github_stargazers = Table(
    "core_github_stargazers",
    usage_metrics_metadata,
    Column(
        "id",
        Integer,
        primary_key=True,
        comment="The unique identifier for each stargazer.",
    ),
    Column(
        "starred_at", DateTime, comment="When the user starred the repository, in UTC."
    ),
    Column("login", String, comment="Github username."),
    Column("node_id", String, comment="The global node ID of the fork in Github."),
    Column("url", String, comment="API link to the user account."),
    Column("html_url", String, comment="HTML link to the user account."),
    Column("followers_url", String, comment="API link to the user's followers."),
    Column(
        "following_url",
        String,
        comment="API link to a list of users that the user is following.",
    ),
    Column("gists_url", String, comment="API link to a list of the user's gists."),
    Column(
        "starred_url",
        String,
        comment="API link to a list of the user's starred repositories.",
    ),
    Column(
        "subscriptions_url",
        String,
        comment="API link to a list of the user's subscriptions.",
    ),
    Column(
        "organizations_url",
        String,
        comment="API link to a list of the user's organizations.",
    ),
    Column(
        "repos_url", String, comment="API link to a list of the user's repositories."
    ),
    Column("events_url", String, comment="API link to a list of the user's events."),
    Column(
        "received_events_url",
        String,
        comment="API link to a list of the user's received events.",
    ),
    Column("type", String, comment="Type of entity (e.g., user)."),
    Column("site_admin", Boolean, comment="Is this user a site admin?"),
)

# See: https://zenodo.org/help/statistics

core_zenodo_logs = Table(
    "core_zenodo_logs",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        Date,
        primary_key=True,
        comment="The date when the metadata was reported.",
    ),
    Column(
        "version",
        String,
        comment="The version (e.g. 10.0.0) of the dataset record.",
    ),
    Column(
        "dataset_slug",
        String,
        comment="The shorthand for the dataset being archived. Matches the pudl_archiver repository dataset slugs when the dataset is archived by the PUDL archiver.",
    ),
    Column(
        "dataset_downloads",
        Integer,
        comment="The total number of downloads for the entire dataset. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
    ),
    Column(
        "dataset_unique_downloads",
        Integer,
        comment="The total number of unique downloads for the entire dataset. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
    ),
    Column(
        "dataset_views",
        Integer,
        comment="The total number of views for the entire dataset. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
    ),
    Column(
        "dataset_unique_views",
        Integer,
        comment="The total number of unique downloads for the entire dataset. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
    ),
    Column(
        "version_downloads",
        Integer,
        comment="The total number of downloads for the version. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.",
    ),
    Column(
        "version_unique_downloads",
        Integer,
        comment="The total number of unique downloads for the version. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.",
    ),
    Column(
        "version_views",
        Integer,
        comment="The total number of views for the version. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.",
    ),
    Column(
        "version_unique_views",
        Integer,
        comment="The total number of unique downloads for the version. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.",
    ),
    Column(
        "version_title",
        String,
        comment="The name of the version in Zenodo.",
    ),
    Column(
        "version_id",
        Integer,
        primary_key=True,
        comment="The unique ID of the Zenodo version. This is identical to the version DOI.",
    ),
    Column(
        "version_record_id",
        Integer,
        comment="The record ID of the Zenodo version. This is identical to the version ID.",
    ),
    Column(
        "concept_record_id",
        Integer,
        comment="The concept record ID. This is shared between all versions of a record.",
    ),
    Column(
        "version_creation_date",
        DateTime,
        comment="The datetime the record was created.",
    ),
    Column(
        "version_last_modified_date",
        DateTime,
        comment="The datetime the record was last modified.",
    ),
    Column(
        "version_last_updated_date",
        DateTime,
        comment="The datetime the record was last updated.",
    ),
    Column(
        "version_publication_date",
        Date,
        comment="The date that the version was published.",
    ),
    Column(
        "version_doi",
        String,
        comment="The DOI of the Zenodo version.",
    ),
    Column(
        "concept_record_doi",
        String,
        comment="The DOI of the Zenodo concept record.",
    ),
    Column(
        "version_doi_url",
        String,
        comment="The DOI link of the Zenodo version.",
    ),
    Column(
        "version_status",
        String,
        comment="The status of the Zenodo version.",
    ),
    Column(
        "version_state",
        String,
        comment="The state of the Zenodo version.",
    ),
    Column(
        "version_submitted",
        Boolean,
        comment="Is the version submitted?",
    ),
    Column(
        "version_description",
        String,
        comment="The description of the version.",
    ),
    Column("partition_key", String),
    Column(
        "software_hash_id",
        String,
        comment="A Software Heritage Software Hash ID (SWHID).",
    ),
)

intake_logs = Table(
    "intake_logs",
    usage_metrics_metadata,
    Column("insert_id", String, primary_key=True, comment="A unique ID for each log."),
    Column("timestamp", DateTime),
    Column("remote_ip", String),
    Column("request_method", String),
    Column("request_uri", String),
    Column("response_status", Integer),
    Column("request_bytes", BigInteger),
    Column("response_bytes", BigInteger),
    Column("response_time_taken", BigInteger),
    Column("request_host", String),
    Column("request_referer", String),
    Column("request_user_agent", String),
    Column("request_operation", String),
    Column("request_bucket", String),
    Column("request_object", String),
    Column("tag", String),
    Column("object_path", String),
    Column("remote_ip_type", String),
    Column("remote_ip_city", String),
    Column("remote_ip_loc", String),
    Column("remote_ip_org", String),
    Column("remote_ip_hostname", String),
    Column("remote_ip_country_name", String),
    Column("remote_ip_asn", String),
    Column("remote_ip_country", String),
    Column("remote_ip_timezone", String),
    Column("remote_ip_latitude", Float),
    Column("remote_ip_longitude", Float),
    Column("remote_ip_postal", String),
    Column("remote_ip_region", String),
    Column("remote_ip_full_location", String),
    Column("partition_key", String),
)

core_eel_hole_log_ins = Table(
    "core_eel_hole_log_ins",
    usage_metrics_metadata,
    Column(
        "insert_id",
        String,
        primary_key=True,
        comment="A unique identifier for the log entry.",
    ),
    Column(
        "timestamp",
        DateTime,
        comment="The time the event described by the log entry occurred.",
    ),
    Column(
        "text_payload",
        String,
        comment="Data provided to the logger in a text format. For the viewer, this includes the redirects generated by a user when logging in.",
    ),
    Column(
        "log_in_query",
        String,
        comment="What was the user doing when they were motivated to log in? This mirrors query when the search prompted a log-in event.",
    ),
    Column("partition_key", String),
)

core_eel_hole_searches = Table(
    "core_eel_hole_searches",
    usage_metrics_metadata,
    Column(
        "insert_id",
        String,
        primary_key=True,
        comment="A unique identifier for the log entry.",
    ),
    Column(
        "user_id",
        String,
        comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
    ),
    Column(
        "user_domain",
        String,
        comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
    ),
    Column(
        "timestamp",
        DateTime,
        comment="The time the event described by the log entry occurred.",
    ),
    Column(
        "query",
        String,
        comment="What did a user type into the search box? This logs periodically, so multiple partial search queries may be logged as someone is typing.",
    ),
    Column(
        "url",
        String,
        comment="What endpoint is a user hitting?",
    ),
    Column(
        "session_id",
        String,
        comment="A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
    ),
    Column("partition_key", String),
)


core_eel_hole_hits = Table(
    "core_eel_hole_hits",
    usage_metrics_metadata,
    Column(
        "insert_id",
        String,
        primary_key=True,
        comment="A unique identifier for the log entry.",
    ),
    Column(
        "timestamp",
        DateTime,
        comment="The time the event described by the log entry occurred.",
    ),
    Column(
        "name",
        String,
        comment="The name of the PUDL table returned by a search query. Only populated for 'hit' event types.",
    ),
    Column(
        "score",
        Float,
        comment="The table's relevance score based on the provided search query. Only populated for 'hit' event types.",
    ),
    Column(
        "tags",
        String,
        comment="The tags associated with a given table in the search results. Only populated for 'hit' events types.",
    ),
    Column("partition_key", String),
)

core_eel_hole_previews = Table(
    "core_eel_hole_previews",
    usage_metrics_metadata,
    Column(
        "insert_id",
        String,
        primary_key=True,
        comment="A unique identifier for the log entry.",
    ),
    Column(
        "user_id",
        String,
        comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
    ),
    Column(
        "user_domain",
        String,
        comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
    ),
    Column(
        "timestamp",
        DateTime,
        comment="The time the event described by the log entry occurred.",
    ),
    Column(
        "url",
        String,
        comment="What endpoint is a user hitting?",
    ),
    Column(
        "params_name",
        String,
        comment="The name of the table being queried using DuckDB.",
    ),
    Column(
        "params_page",
        Integer,
        comment="The page of the query.",
    ),
    Column(
        "params_per_page",
        Integer,
        comment="The number of records returned per DuckDB query. This is set by us, so it should be expected to hold constant without our intervention.",
    ),
    Column(
        "params_filters_field_name",
        String,
        comment="The variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_field_type",
        String,
        comment="The data type of the variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_operation",
        String,
        comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
    ),
    Column(
        "params_filters_value",
        String,
        comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
    ),
    Column(
        "params_filters_field_name_1",
        String,
        comment="The variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_field_type_1",
        String,
        comment="The data type of the variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_operation_1",
        String,
        comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
    ),
    Column(
        "params_filters_value_1",
        String,
        comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
    ),
    Column(
        "params_filters_field_name_2",
        String,
        comment="The variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_field_type_2",
        String,
        comment="The data type of the variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_operation_2",
        String,
        comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
    ),
    Column(
        "params_filters_value_2",
        String,
        comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
    ),
    Column(
        "params_filters_field_name_3",
        String,
        comment="The variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_field_type_3",
        String,
        comment="The data type of the variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_operation_3",
        String,
        comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
    ),
    Column(
        "params_filters_value_3",
        String,
        comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
    ),
    Column(
        "params_filters_field_name_4",
        String,
        comment="The variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_field_type_4",
        String,
        comment="The data type of the variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_operation_4",
        String,
        comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
    ),
    Column(
        "params_filters_value_4",
        String,
        comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
    ),
    Column(
        "params_filters_field_name_5",
        String,
        comment="The variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_field_type_5",
        String,
        comment="The data type of the variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_operation_5",
        String,
        comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
    ),
    Column(
        "params_filters_value_5",
        String,
        comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
    ),
    Column(
        "params_filters_field_name_6",
        String,
        comment="The variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_field_type_6",
        String,
        comment="The data type of the variable on which a user is performing a filter using DuckDB.",
    ),
    Column(
        "params_filters_operation_6",
        String,
        comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
    ),
    Column(
        "params_filters_value_6",
        String,
        comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
    ),
    Column(
        "session_id",
        String,
        comment="A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
    ),
    Column("partition_key", String),
)

core_eel_hole_downloads = (
    Table(
        "core_eel_hole_downloads",
        usage_metrics_metadata,
        Column(
            "insert_id",
            String,
            primary_key=True,
            comment="A unique identifier for the log entry.",
        ),
        Column(
            "user_id",
            String,
            comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
        ),
        Column(
            "user_domain",
            String,
            comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
        ),
        Column(
            "timestamp",
            DateTime,
            comment="The time the event described by the log entry occurred.",
        ),
        Column(
            "url",
            String,
            comment="What endpoint is a user hitting?",
        ),
        Column(
            "params_name",
            String,
            comment="The name of the table being queried using DuckDB.",
        ),
        Column(
            "params_page",
            Integer,
            comment="The page of the query.",
        ),
        Column(
            "params_per_page",
            Integer,
            comment="The number of records returned per DuckDB query. This is set by us, so it should be expected to hold constant without our intervention.",
        ),
        Column(
            "params_filters_field_name",
            String,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_field_type",
            String,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_operation",
            String,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        Column(
            "params_filters_value",
            String,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
        Column(
            "params_filters_field_name_1",
            String,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_field_type_1",
            String,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_operation_1",
            String,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        Column(
            "params_filters_value_1",
            String,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
        Column(
            "params_filters_field_name_2",
            String,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_field_type_2",
            String,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_operation_2",
            String,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        Column(
            "params_filters_value_2",
            String,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
        Column(
            "params_filters_field_name_3",
            String,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_field_type_3",
            String,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_operation_3",
            String,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        Column(
            "params_filters_value_3",
            String,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
        Column(
            "params_filters_field_name_4",
            String,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_field_type_4",
            String,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_operation_4",
            String,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        Column(
            "params_filters_value_4",
            String,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
        Column(
            "params_filters_field_name_5",
            String,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_field_type_5",
            String,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_operation_5",
            String,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        Column(
            "params_filters_value_5",
            String,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
        Column(
            "params_filters_field_name_6",
            String,
            comment="The variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_field_type_6",
            String,
            comment="The data type of the variable on which a user is performing a filter using DuckDB.",
        ),
        Column(
            "params_filters_operation_6",
            String,
            comment="The operation performed on the variable a user is using to perform a filter using DuckDB (e.g., greater than, contains).",
        ),
        Column(
            "params_filters_value_6",
            String,
            comment="The value that a user is using to perform a filter using DuckDB (e.g., greater than 2017, contains 'natural gas').",
        ),
        Column(
            "session_id",
            String,
            comment="A session ID for a logged in user. A new session is created after a user has been inactive for 30 minutes.",
        ),
        Column("partition_key", String),
    ),
)
core_eel_hole_user_settings_updates = Table(
    "core_eel_hole_user_settings_updates",
    usage_metrics_metadata,
    Column(
        "insert_id",
        String,
        primary_key=True,
        comment="A unique identifier for the log entry.",
    ),
    Column(
        "user_id",
        String,
        comment="The unique ID identifying a logged-in user's activity. Implemented 09-2025.",
    ),
    Column(
        "user_domain",
        String,
        comment="User's email domain - the part of a user's email address that follows the '@' symbol.",
    ),
    Column(
        "timestamp",
        DateTime,
        comment="The time the event described by the log entry occurred.",
    ),
    Column(
        "accepted",
        Boolean,
        comment="Has a user accepted the privacy policy?",
    ),
    Column(
        "newsletter",
        Boolean,
        comment="Has a user subscribed to the newsletter?",
    ),
    Column(
        "outreach",
        Boolean,
        comment="Has a user agreed to be contacted for further discussion about PUDL?",
    ),
    Column("partition_key", String),
)
