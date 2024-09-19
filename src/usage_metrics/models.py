"""SQLAlchemy models for usage_metrics database."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
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

core_s3_logs = Table(
    "core_s3_logs",
    usage_metrics_metadata,
    Column("id", String, primary_key=True, comment="A unique ID for each log."),
    # Query information
    Column("time", DateTime),
    Column("request_uri", String),
    Column("operation", String),
    Column("bucket", String),
    Column("bucket_owner", String),
    Column("requester", String),
    Column("http_status", Integer),
    Column("megabytes_sent", Float),
    # IP location
    Column("remote_ip", String),
    Column("remote_ip_city", String),
    Column("remote_ip_loc", String),
    Column("remote_ip_org", String),
    Column("remote_ip_hostname", String),
    Column("remote_ip_country_name", String),
    Column("remote_ip_asn", String),
    Column("remote_ip_bogon", Boolean),
    Column("remote_ip_country", String),
    Column("remote_ip_timezone", String),
    Column("remote_ip_latitude", Float),
    Column("remote_ip_longitude", Float),
    Column("remote_ip_postal", String),
    Column("remote_ip_region", String),
    Column("remote_ip_full_location", String),
    # Other reported context
    Column("access_point_arn", String),
    Column("acl_required", String),
    Column("authentication_type", String),
    Column("cipher_suite", String),
    Column("error_code", String),
    Column("host_header", String),
    Column("host_id", String),
    Column("key", String),
    Column("object_size", Float),
    Column("request_id", String),
    Column("referer", String),
    Column("signature_version", String),
    Column("tls_version", String),
    Column("total_time", BigInteger),
    Column("turn_around_time", Float),
    Column("user_agent", String),
    Column("version_id", String),
    Column("partition_key", String),
)

out_s3_logs = Table(
    "out_s3_logs",
    usage_metrics_metadata,
    Column("id", String, primary_key=True, comment="A unique ID for each log."),
    # Query information
    Column("time", DateTime),
    Column("table", String),
    Column("version", String),
    # IP location
    Column("remote_ip", String),
    Column("remote_ip_city", String),
    Column("remote_ip_loc", String),
    Column("remote_ip_org", String),
    Column("remote_ip_hostname", String),
    Column("remote_ip_country_name", String),
    Column("remote_ip_asn", String),
    Column("remote_ip_bogon", Boolean),
    Column("remote_ip_country", String),
    Column("remote_ip_timezone", String),
    Column("remote_ip_latitude", Float),
    Column("remote_ip_longitude", Float),
    Column("remote_ip_postal", String),
    Column("remote_ip_region", String),
    Column("remote_ip_full_location", String),
    # Other reported context
    Column("access_point_arn", String),
    Column("acl_required", String),
    Column("authentication_type", String),
    Column("megabytes_sent", Float),
    Column("cipher_suite", String),
    Column("error_code", String),
    Column("host_header", String),
    Column("host_id", String),
    Column("http_status", Integer),
    Column("key", String),
    Column("object_size", Float),
    Column("referer", String),
    Column("request_id", String),
    Column("request_uri", String),
    Column("signature_version", String),
    Column("tls_version", String),
    Column("total_time", BigInteger),
    Column("turn_around_time", Float),
    Column("user_agent", String),
    Column("version_id", String),
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
    Column("total_views", Integer),
    Column("total_downloads", Integer),
    Column("total_votes", Integer),
    Column("usability_rating", Float),
    # Metadata on dataset
    Column("dataset_name", String),
    Column("owner", String),
    Column("title", String),
    Column("subtitle", String),
    Column("description", String),
    Column("keywords", String),
    Column("dataset_id", String),
    Column("is_private", String),
    Column("licenses", String),
    Column("collaborators", String),
    Column("data", String),
    Column("partition_key", String),
)

core_github_popular_referrers = Table(
    "core_github_popular_referrers",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        DateTime,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column("referrer", String, primary_key=True, comment="The unique referrer."),
    Column("total_referrals", Integer),
    Column("unique_referrals", Integer),
    Column("partition_key", String),
)

core_github_popular_paths = Table(
    "core_github_popular_paths",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        DateTime,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column(
        "path",
        String,
        primary_key=True,
        comment="One of the ten most popular Github paths on a given date.",
    ),
    Column("title", String),
    Column("total_views", Integer),
    Column("unique_views", Integer),
    Column("partition_key", String),
)

core_github_clones = Table(
    "core_github_clones",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        DateTime,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column("total_clones", Integer),
    Column("unique_clones", Integer),
    Column("partition_key", String),
)

core_github_views = Table(
    "core_github_views",
    usage_metrics_metadata,
    Column(
        "metrics_date",
        DateTime,
        primary_key=True,
        comment="The date for each metrics snapshot.",
    ),
    Column("total_views", Integer),
    Column("unique_views", Integer),
    Column("partition_key", String),
)

core_github_forks = Table(
    "core_github_forks",
    usage_metrics_metadata,
    Column(
        "id", Integer, primary_key=True, comment="The unique identifier for each fork."
    ),
    Column("node_id", String),
    Column("name", String),
    Column("full_name", String),
    Column("private", Boolean),
    Column("owner", String),
    Column("description", String),
    Column("url", String),
    Column("created_at", DateTime),
    Column("updated_at", DateTime),
    Column("pushed_at", DateTime),
    Column("homepage", String),
    Column("size_kb", Integer),
    Column("stargazers_count", Integer),
    Column("watchers_count", Integer),
    Column("language", String),
    Column("has_issues", Boolean),
    Column("has_projects", Boolean),
    Column("has_downloads", Boolean),
    Column("has_wiki", Boolean),
    Column("has_pages", Boolean),
    Column("has_discussions", Boolean),
    Column("forks_count", Integer),
    Column("archived", Boolean),
    Column("disabled", Boolean),
    Column("license", String),
    Column("allow_forking", Boolean),
    Column("is_template", Boolean),
    Column("web_commit_signoff_required", Boolean),
    Column("topics", String),
    Column("visibility", String),
    Column("forks", Integer),
    Column("open_issues", Integer),
    Column("watchers", Integer),
    Column("default_branch", String),
    Column("permissions", String),
)

core_github_stargazers = Table(
    "core_github_stargazers",
    usage_metrics_metadata,
    Column(
        "id",
        Integer,
        primary_key=True,
        comment="The unique identifier for each stargazer.",
    ),
    Column("starred_at", DateTime),
    Column("login", String),
    Column("node_id", String),
    Column("url", String),
    Column("html_url", String),
    Column("followers_url", String),
    Column("following_url", String),
    Column("gists_url", String),
    Column("starred_url", String),
    Column("subscriptions_url", String),
    Column("organizations_url", String),
    Column("repos_url", String),
    Column("events_url", String),
    Column("received_events_url", String),
    Column("type", String),
    Column("site_admin", Boolean),
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
