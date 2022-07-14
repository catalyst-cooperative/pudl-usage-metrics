"""SQLAlchemy models for usage_metrics database."""
from sqlalchemy import (
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
    Column("remote_ip_latitude", String),
    Column("remote_ip_longitude", String),
    Column("remote_ip_postal", String),
    Column("remote_ip_region", String),
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
    Column("request_bytes", Integer),
    Column("response_bytes", Integer),
    Column("response_time_taken", Integer),
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
    Column("remote_ip_latitude", String),
    Column("remote_ip_longitude", String),
    Column("remote_ip_postal", String),
    Column("remote_ip_region", String),
)
