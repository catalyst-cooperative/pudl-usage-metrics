"""Dagster software defined assets for datasette logs."""
import json
from pathlib import Path

import pandas as pd
import pandas_gbq
from dagster import AssetMaterialization, op
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials

from usage_metrics.helpers import geocode_ip, parse_request_url
from usage_metrics.schemas.datasette import EMPTY_COLUMNS

JSON_FIELDS = ["resource", "http_request", "labels"]
DATA_PATHS = ["/pudl", "/ferc1", "pudl.db", "ferc1.db", ".json"]

GCP_PROJECT_ID = "catalyst-cooperative-pudl"

SERVICE_ACCOUNT_KEY_PATH = (
    Path(__file__).parent.parent / "bigquery-service-account-key.json"
)


def get_bq_credentials() -> Credentials:
    """Get credentials object for datasette-logs-viewer service account."""
    try:
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH
        )
    except FileNotFoundError:
        FileNotFoundError("Can't find the service account key json file.")


@op()
def extract(context) -> pd.DataFrame:
    """Extract Datasette logs from BigQuery instance."""
    credentials = get_bq_credentials()

    context.log.info(dir(context))
    context.log.info(context.op_config)
    start_date = context.op_config["start_date"]
    end_date = context.op_config["end_date"]

    raw_logs = pandas_gbq.read_gbq(
        "SELECT * FROM `datasette_logs.run_googleapis_com_requests`"
        f" WHERE DATE(timestamp) >= '{start_date}' AND DATE(timestamp) < '{end_date}'",
        project_id=GCP_PROJECT_ID,
        credentials=credentials,
    )
    context.log.info(raw_logs.timestamp.describe())
    context.log.info(raw_logs.shape)

    # Convert CamelCase to snake_case
    raw_logs.columns = raw_logs.columns.str.replace(r"(?<!^)(?=[A-Z])", "_").str.lower()

    # Convert the JSON fields into str because sqlalchemy can't write dicts to json
    for field in JSON_FIELDS:
        raw_logs[field] = raw_logs[field].apply(json.dumps)

    # Remove the UTC timezone from datetime columns
    for field in raw_logs.select_dtypes(include=["datetimetz"]):
        raw_logs[field] = raw_logs[field].dt.tz_localize(None)

    return raw_logs


@op()
def unpack_httprequests(raw_logs: pd.DataFrame) -> pd.DataFrame:
    """Unpack http_request dict keys into separate fields."""
    # Convert the JSON strings back to dicts
    for field in JSON_FIELDS:
        raw_logs[field] = raw_logs[field].apply(json.loads)

    raw_logs["trace_sampled"] = raw_logs["trace_sampled"].astype(pd.BooleanDtype())

    # There are a couple of duplicates due to overlap between
    # properly saved logs starting 3/1/22 and the old logs from
    # 1/31/22 to 3/1/22
    logs = raw_logs.drop_duplicates(subset=["insert_id"])
    logs = logs.set_index("insert_id")
    assert logs.index.is_unique

    # Unpack http_request json keys to columns
    http_request_df = pd.DataFrame.from_dict(
        logs.http_request.to_dict(), orient="index"
    )
    unpacked_logs = pd.concat([http_request_df, logs], axis=1)
    assert len(unpacked_logs) == len(logs)
    assert unpacked_logs.index.is_unique

    unpacked_logs.index.name = "insert_id"
    unpacked_logs = unpacked_logs.reset_index()

    # Convert the new columns to snake_case
    unpacked_logs.columns = unpacked_logs.columns.str.replace(
        r"(?<!^)(?=[A-Z])", "_"
    ).str.lower()

    # Convert the JSON fields into str because sqlalchemy can't write dicts to json
    for field in JSON_FIELDS:
        unpacked_logs[field] = unpacked_logs[field].apply(json.dumps)

    return unpacked_logs


@op()
def parse_urls(context, df: pd.DataFrame) -> pd.DataFrame:
    """Parse the request url into component parts."""
    # Remove columns that don't contain any data
    df = df.drop(columns=EMPTY_COLUMNS)

    # Parse the request url
    df = df.set_index("insert_id")
    parsed_requests = df.request_url.apply(lambda x: parse_request_url(x)).to_frame()
    parsed_requests = pd.DataFrame.from_dict(
        parsed_requests.request_url.to_dict(), orient="index"
    )
    parsed_requests.columns = ["request_url_" + col for col in parsed_requests.columns]

    # Clean up the component url fields
    for field in parsed_requests.columns:
        parsed_requests[field] = parsed_requests[field].replace("", pd.NA)

    # Add the component fields back to the logs
    parsed_logs = pd.concat([df, parsed_requests], axis=1)
    parsed_logs.index.name = "insert_id"
    parsed_logs = parsed_logs.reset_index()
    assert len(df) == len(parsed_logs)
    return parsed_logs


@op()
def geocode_ips(context, df: pd.DataFrame) -> pd.DataFrame:
    """Geocode the ip addresses."""
    # Geocode the remote ip addresses
    context.log.info("Geocoding ip addresses.")
    # Instead of geocoding every log, geocode the distinct ips
    unique_ips = pd.Series(df.remote_ip.unique())
    geocoded_ips = unique_ips.apply(lambda ip: geocode_ip(ip))
    geocoded_ips = pd.DataFrame.from_dict(geocoded_ips.to_dict(), orient="index")
    geocoded_ip_column_map = {
        col: "remote_ip_" + col for col in geocoded_ips.columns if col != "ip"
    }
    geocoded_ip_column_map["ip"] = "remote_ip"
    geocoded_ips = geocoded_ips.rename(columns=geocoded_ip_column_map)

    # Split the org and org ASN into different columns
    geocoded_ips["remote_ip_asn"] = geocoded_ips.remote_ip_org.str.split(" ").str[0]
    geocoded_ips["remote_ip_org"] = (
        geocoded_ips.remote_ip_org.str.split(" ").str[1:].str.join(sep=" ")
    )

    # Add the component fields back to the logs
    clean_logs = df.merge(geocoded_ips, on="remote_ip", how="left", validate="m:1")
    return clean_logs


@op(required_resource_keys={"database_manager"})
def load(context, clean_datasette_logs: pd.DataFrame):
    """
    Filter the useful data request logs.

    Most requests are for js and css assets, we are more interested in
    which paths folks are requesting. This asset contains requests for
    ferc1 and pudl data.

    This asset also removes columns not needed for analysis.
    """
    data_request_logs = clean_datasette_logs[
        clean_datasette_logs.request_url_path.str.contains("|".join(DATA_PATHS))
    ]
    context.resources.database_manager.append_df_to_table(
        data_request_logs, "data_request_logs"
    )
    context.log_event(
        AssetMaterialization(
            asset_key="data_request_logs",
            description="Clean data request logs from datasette.",
            partition=context.get_mapping_key(),
            metadata={
                "Number of Rows:": len(data_request_logs),
                "Min Date": str(data_request_logs.timestamp.min()),
                "Max Date": str(data_request_logs.timestamp.max()),
            },
        )
    )
