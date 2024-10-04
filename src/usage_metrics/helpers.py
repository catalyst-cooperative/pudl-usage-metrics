"""General utility functions for cleaning usage metrics data."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse

import ipinfo
import pandas as pd
import requests
from dagster import OutputContext, RetryPolicy, op
from joblib import Memory

cache_dir = Path(__file__).parents[2] / "cache"
cache_dir.mkdir(exist_ok=True)
ip_address_cache = Memory(cache_dir, verbose=0)

REQUEST_TIMEOUT = 10


@ip_address_cache.cache
def geocode_ip(ip_address: str) -> dict:
    """Geocode an ip address using ipinfo API.

    This function uses joblib to cache api calls so we only have to
    call the api once for a given ip address. We get 50k free api calls.

    Args:
        ip_address: An ip address.

    Return:
        details: Ip location and org information.
    """
    try:
        ipinfo_token = os.environ["IPINFO_TOKEN"]
    except KeyError:
        raise AssertionError("Can't find IPINFO_TOKEN.")
    handler = ipinfo.getHandler(
        ipinfo_token, request_options={"timeout": REQUEST_TIMEOUT}
    )

    details = handler.getDetails(ip_address)
    return details.all


@op(retry_policy=RetryPolicy(max_retries=5))
def geocode_ips(df: pd.DataFrame) -> pd.DataFrame:
    """Geocode the ip addresses using ipinfo API.

    This op geocodes the users ip address to get useful
    information like ip location and organization.

    Args:
        df: dataframe with a remote_ip column.

    Returns:
        geocoded_logs: dataframe with ip location info columns.
    """
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

    # Create a verbose ip location field
    geocoded_ips["remote_ip_full_location"] = (
        geocoded_ips.remote_ip_city
        + ", "
        + geocoded_ips.remote_ip_region
        + ", "
        + geocoded_ips.remote_ip_country
    )

    # Add the component fields back to the logs
    # TODO: Could create a separate db table for ip information.
    # I'm not sure if IP addresses always geocode to the same information.
    geocoded_logs = df.merge(geocoded_ips, on="remote_ip", how="left", validate="m:1")
    return geocoded_logs


def parse_request_url(url: str) -> dict:
    """Create dictionary of request components.

    Args:
        url: A generic url.

    Returns:
        The parsed URL components.
    """
    pr = urlparse(url)
    return {
        "scheme": pr.scheme,
        "netloc": pr.netloc,
        "path": pr.path,
        "query": pr.query,
    }


def convert_camel_case_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """Convert CamelCase columns of a dataframe to snake_case.

    Args:
        df: A dataframe with CamelCase columns.

    Returns:
        df: A dataframe with snake_case columns.
    """
    df.columns = df.columns.str.replace(r"(?<!^)(?=[A-Z])", "_").str.lower()
    return df


def unpack_json_series(series: pd.Series) -> pd.DataFrame:
    """Unpack a series containing json records to a DataFrame.

    Expects no more than one json record per series element.

    Args:
        series: A pandas series on json records.

    Returns:
        unpacked_df: A dataframe where columns are the fields of the json records.
    """
    series_dict = series.to_dict()
    # Replace missing data with empty dicts
    series_dict = {index: v if v else {} for index, v in series_dict.items()}

    unpacked_df = pd.DataFrame.from_dict(series_dict, orient="index")
    assert len(unpacked_df) <= len(
        series
    ), "Unpacked more JSON records than there are records in the DataFrame."
    return unpacked_df


def str_to_datetime(
    date: str, fmt: str = "%Y-%m-%d", tzinfo: timezone = UTC
) -> datetime:
    """Convert a string to a date."""
    return datetime.strptime(date, fmt).replace(tzinfo=tzinfo)


def get_table_name_from_context(context: OutputContext) -> str:
    """Retrieves the table name from the context object."""
    if context.has_asset_key:
        return context.asset_key.to_python_identifier()
    return context.get_identifier()


def make_request(
    url: str, headers: str | None = None, params: str | None = None, timeout: int = 100
) -> requests.models.Response:
    """Makes a request with some error handling.

    Args:
        query (str): A request url.
        headers (str): Header to include in the request.
        params (str): Params of request.
        timeout (int): Timeout of request (in seconds).

    Returns:
        response (requests.models.Response): the request response.
    """
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)

        response.raise_for_status()
    except HTTPError as http_err:
        raise HTTPError(
            f"HTTP error occurred: {http_err}\n\tResponse text: {response.text}"
        )
    return response
