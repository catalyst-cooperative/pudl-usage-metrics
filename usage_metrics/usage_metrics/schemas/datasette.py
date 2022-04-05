"""Pandera schemas for datasette SDAs."""
import copy

import pandas as pd
from pandera import Column, DataFrameSchema

raw_logs = DataFrameSchema(
    {
        "log_name": Column(
            str, description="The path to the google resource and log type."
        ),
        "resource": Column(
            str,
            description=(
                "String of JSON containing information about the google" " resource."
            ),
        ),
        "text_payload": Column(
            str,
            nullable=True,
            description="Messages from Google logs. Typically error messages.",
        ),
        "timestamp": Column(
            pd.DatetimeTZDtype(tz="UTC"), description="Time request was sent."
        ),
        "receive_timestamp": Column(
            pd.DatetimeTZDtype(tz="UTC"), description="Time request was recieved."
        ),
        "severity": Column(str, description="Log level."),
        "insert_id": Column(str, description="A unique ID for each log."),
        "http_request": Column(
            str, description="String of JSON containing the http request."
        ),
        "labels": Column(
            str, description="String of JSON containing Google label information."
        ),
        "operation": Column(str, nullable=True),
        "trace": Column(str, description="The trace ID.", nullable=True),
        "span_id": Column(str, nullable=True),
        "trace_sampled": Column(pd.BooleanDtype, nullable=True),
        "source_location": Column(str, nullable=True),
    },
    strict=True,
    name="raw_logs",
)

unpack_httprequests = raw_logs.add_columns(
    {
        "cache_hit": Column(str, nullable=True),
        "cache_lookup": Column(str, nullable=True),
        "request_url": Column(
            str,
            description=(
                "The request URL sent to our datasette instance. These requests "
                "include html and data requests via datasette's API."
            ),
        ),
        "protocol": Column(str, description="The transfer protocol."),
        "cache_fill_bytes": Column(str, nullable=True),
        "response_size": Column(
            float, nullable=True, description="The size of the response in bytes."
        ),
        "server_ip": Column(str, description="The Cloud Run instance IP address."),
        "cache_validated_with_origin_server": Column(str, nullable=True),
        "request_method": Column(
            str, description="The request HTTP method (GET, POST, HEAD, ...)"
        ),
        "request_size": Column(int, description="The size  of the request in bytes."),
        "user_agent": Column(
            str,
            nullable=True,
            description="User agent information like OS and browser versions.",
        ),
        "status": Column(int, description="Request response status."),
        "referer": Column(
            str, nullable=True, description="The url of the refering website."
        ),
        "latency": Column(float),
        "remote_ip": Column(
            str, description="The IP address of the user sending requests to datasette."
        ),
    }
)
unpack_httprequests.name = "unpack_httprequests"

# clean_datasette_logs
EMPTY_COLUMNS = [
    "cache_lookup",
    "cache_hit",
    "cache_validated_with_origin_server",
    "cache_fill_bytes",
    "operation",
    "span_id",
    "trace_sampled",
    "source_location",
]

clean_datasette_logs = unpack_httprequests.remove_columns(EMPTY_COLUMNS)
clean_datasette_logs = clean_datasette_logs.add_columns(
    {
        "request_url_path": Column(
            str,
            description=(
                "Hierarchical path of the request. The path information about "
                "which PUDL resources people are accessing."
            ),
        ),
        "request_url_query": Column(
            str,
            nullable=True,
            description=(
                "Query component of the request. Contains the datasette sql query "
                "if it exists."
            ),
        ),
        "request_url_scheme": Column(str, description="URL scheme specifier."),
        "request_url_netloc": Column(
            str,
            description=(
                "Network location part. This is data.catalyst.coop for all " "records."
            ),
        ),
        "remote_ip_city": Column(str, description="The user's city.", nullable=True),
        "remote_ip_loc": Column(
            str,
            description=(
                "The user's lat, long location. I think this is the lat long "
                "of the city center."
            ),
        ),
        "remote_ip_org": Column(
            str,
            description=(
                "The network organization name. Most internet providres but some "
                "institutions have their own network."
            ),
            nullable=True,
        ),
        "remote_ip_hostname": Column(str, nullable=True),
        "remote_ip_country_name": Column(
            str, description="The user's full country name.", nullable=True
        ),
        "remote_ip_asn": Column(
            str, description="The network's ASN number.", nullable=True
        ),
        "remote_ip_country": Column(
            str, description="The user's country abreviation.", nullable=True
        ),
        "remote_ip_timezone": Column(
            str, description="The user's timezone.", nullable=True
        ),
        "remote_ip_latitude": Column(
            str, description="The user's latitude.", nullable=True
        ),
        "remote_ip_longitude": Column(
            str, description="The user's longitude.", nullable=True
        ),
        "remote_ip_postal": Column(
            str, nullable=True, description="The user's postal code."
        ),
        "remote_ip_region": Column(
            str,
            description="The user's region. These are typically states or provinces.",
            nullable=True,
        ),
    }
)
clean_datasette_logs.name = "clean_datasette_logs"

data_request_logs = copy.deepcopy(clean_datasette_logs)
data_request_logs.name = "data_request_logs"
