"""General utility functions for cleaning usage metrics data."""

from __future__ import annotations

import os
import time
from functools import wraps
from pathlib import Path
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

# Fields pulled from ipinfo's Lite API response (plus its client-side
# country-name/bogon lookups) and the usage_metrics column each maps to.
# Lite doesn't return city/region/loc/postal/timezone/hostname or a combined
# `org` string the way the old Core API did -- it gives `asn` and `as_name`
# as separate fields already, so no more string-splitting is needed.
_IPINFO_FIELD_MAP = {
    "ip": "remote_ip",
    "country_code": "remote_ip_country",
    "country_name": "remote_ip_country_name",
    "as_name": "remote_ip_org",
    "asn": "remote_ip_asn",
    "bogon": "remote_ip_bogon",
}


@ip_address_cache.cache
def geocode_ip(ip_address: str) -> dict:
    """Geocode an ip address using ipinfo's Lite API.

    This function uses joblib to cache api calls so we only have to
    call the api once for a given ip address.

    Args:
        ip_address: An ip address.

    Return:
        details: Ip location and org information.
    """
    try:
        ipinfo_token = os.environ["IPINFO_TOKEN"]
    except KeyError:
        raise AssertionError("Can't find IPINFO_TOKEN.")
    handler = ipinfo.getHandlerLite(
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
    unique_ips = pd.Series(df.remote_ip.dropna().unique())
    geocoded_ips = unique_ips.apply(lambda ip: geocode_ip(ip))
    geocoded_ips = pd.DataFrame.from_dict(geocoded_ips.to_dict(), orient="index")

    # Keep only the fields the Lite API actually provides (reindex adds any
    # that are missing -- e.g. a bogon IP's response has no asn/country -- as
    # NaN, instead of raising a KeyError) and rename them to their usage_metrics
    # column names.
    geocoded_ips = geocoded_ips.reindex(columns=_IPINFO_FIELD_MAP.keys())
    geocoded_ips = geocoded_ips.rename(columns=_IPINFO_FIELD_MAP)

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
    df.columns = df.columns.str.replace(r"(?<!^)(?=[A-Z])", "_", regex=True).str.lower()
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
    assert len(unpacked_df) <= len(series), (
        "Unpacked more JSON records than there are records in the DataFrame."
    )
    return unpacked_df


def get_table_name_from_context(context: OutputContext) -> str:
    """Retrieves the table name from the context object."""
    if context.has_asset_key:
        return context.asset_key.to_python_identifier()
    return context.get_identifier()


def retry_request(retries: int = 3, delay: int = 2, backoff: int = 2):
    """Define a decorator to retry requests with an exponential backoff.

    The first backoff will be 2 seconds, the second 2*2 seconds and so-on.

    Args:
        retries: how many retries to attempt.
        delay: original number of seconds to wait before retrying.
        backoff: the exponent by which to increase the backoff each time.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal delay  # Make ruff happy
            attempts = 0
            while attempts < retries:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException:
                    attempts += 1
                    time.sleep(delay)
                    delay *= backoff  # Exponential backoff
            raise RuntimeError("Max retries reached")

        return wrapper

    return decorator
