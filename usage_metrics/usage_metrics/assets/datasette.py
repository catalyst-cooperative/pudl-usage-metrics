import json
import os
import pandas as pd
import pydata_google_auth
import pandas_gbq
import ipinfo
import logging

from urllib.parse import urlparse
from dagster import AssetGroup, io_manager, op, Out, asset
from google.oauth2 import service_account
from pathlib import Path

from usage_metrics.resources.postgres import DataframePostgresIOManager
from usage_metrics.helpers import parse_request_url, geocode_ip


JSON_FIELDS = ["resource", "http_request", "labels"]
DATA_PATHS = ["/pudl", "/ferc1", "pudl.db", "ferc1.db", ".json"]

@asset
def raw_logs():
    """Extract Datasette logs from BigQuery instance."""
    credentials = service_account.Credentials.from_service_account_file("/app/bigquery-service-account-key.json")

    project_id = 'catalyst-cooperative-pudl'

    raw_logs = pandas_gbq.read_gbq(
        "SELECT * FROM `datasette_logs.run_googleapis_com_requests`",
        project_id=project_id,
        credentials=credentials,
    )

    # Convert CamelCase to snake_case
    raw_logs.columns = raw_logs.columns.str.replace(r'(?<!^)(?=[A-Z])', '_').str.lower()

    # Convert the JSON fields into str because sqlalchemy can't write dicts to json
    for field in JSON_FIELDS:
        raw_logs[field] = raw_logs[field].apply(json.dumps)
    return raw_logs

@asset
def unpack_httprequests(raw_logs):
    """Unpack http_request dict keys into separate fields."""
    # Convert the JSON fields into str because sqlalchemy can't write dicts to json
    for field in JSON_FIELDS:
        raw_logs[field] = raw_logs[field].apply(json.loads)

    # There are a couple of duplicates due to overlap between
    # properly saved logs starting 3/1/22 and the old logs from
    # 1/31/22 to 3/1/22
    logs = raw_logs.drop_duplicates(subset=["insert_id"])
    logs = logs.set_index("insert_id")
    assert logs.index.is_unique

    http_request_df = pd.DataFrame.from_dict(logs.http_request.to_dict(), orient='index')
    unpacked_logs = pd.concat([http_request_df, logs], axis=1)
    assert len(unpacked_logs) == len(logs)
    assert unpacked_logs.index.is_unique

    unpacked_logs.index.name = "insert_id"
    unpacked_logs = unpacked_logs.reset_index()

    unpacked_logs.columns = unpacked_logs.columns.str.replace(r'(?<!^)(?=[A-Z])', '_').str.lower()

    for field in JSON_FIELDS:
        unpacked_logs[field] = unpacked_logs[field].apply(json.dumps)

    return unpacked_logs

@asset
def clean_datasette_logs(unpack_httprequests: pd.DataFrame):
    """
    Clean the raw logs.
    
    Transformations:
    - unpack the request url into component parts
    - geocode the ip addresses
    """
    # Parse the request url
    unpack_httprequests = unpack_httprequests.set_index("insert_id")
    parsed_requests = unpack_httprequests.request_url.apply(lambda x: parse_request_url(x)).to_frame()
    parsed_requests = pd.DataFrame.from_dict(parsed_requests.request_url.to_dict(), orient='index')
    parsed_requests.columns = ["request_url_" + col for col in parsed_requests.columns]
    
    # Clean up the component url fields
    for field in parsed_requests.columns:
        parsed_requests[field] = parsed_requests[field].replace("", pd.NA)

    # Add the component fields back to the logs
    parsed_logs = pd.concat([unpack_httprequests, parsed_requests], axis=1)
    parsed_logs.index.name = "insert_id"
    parsed_logs = parsed_logs.reset_index()
    assert len(unpack_httprequests) == len(parsed_logs)

    ### Geocode the remote ip addresses
    logging.info("Geocoding ip addresses.")
    # Instead of geocoding every log, geocode the distinct ips
    unique_ips = pd.Series(parsed_logs.remote_ip.unique())
    geocoded_ips = unique_ips.apply(lambda ip: geocode_ip(ip))
    geocoded_ips = pd.DataFrame.from_dict(geocoded_ips.to_dict(), orient='index')
    geocoded_ip_column_map = {col: "remote_ip_" + col for col in geocoded_ips.columns if col != "ip"}
    geocoded_ip_column_map["ip"] = "remote_ip"
    geocoded_ips = geocoded_ips.rename(columns=geocoded_ip_column_map)

    # Split the org and org ASN into different columns
    geocoded_ips["remote_ip_asn"] = geocoded_ips.remote_ip_org.str.split(" ").str[0]
    geocoded_ips["remote_ip_org"] = geocoded_ips.remote_ip_org.str.split(" ").str[1:].str.join(sep=" ")

    # Add the component fields back to the logs
    clean_logs = parsed_logs.merge(geocoded_ips, on="remote_ip", how="left", validate="m:1")

    return clean_logs

@asset
def data_request_logs(clean_datasette_logs):
    """
    Filter the useful data request logs.

    Most requests are for js and css assets, we are more interested in 
    which paths folks are requesting. This asset contains requests for
    ferc1 and pudl data.

    This asset also removes columns not needed for analysis.
    """
    data_request_logs = clean_datasette_logs[clean_datasette_logs.request_url_path.str.contains("|".join(DATA_PATHS))]
    return data_request_logs

@io_manager
def df_to_postgres_io_manager(_):
    return DataframePostgresIOManager()

datasette_asset_group = AssetGroup([unpack_httprequests, raw_logs, clean_datasette_logs, data_request_logs], resource_defs={"io_manager": df_to_postgres_io_manager})