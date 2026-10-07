"""General utility functions for cleaning usage metrics data."""

import os
import time
from functools import wraps

import ipinfo
import pandas as pd
import requests
from dagster import OutputContext, RetryPolicy, op
from joblib import Memory

from usage_metrics.paths import get_ipinfo_cache_dir

REQUEST_TIMEOUT = 10

# Fields pulled from ipinfo's Lite API response (plus its client-side
# country-name/bogon lookups) and the usage_metrics column each maps to.
_IPINFO_FIELD_MAP = {
    "ip": "remote_ip",
    "country_code": "remote_ip_country",
    "country_name": "remote_ip_country_name",
    "as_name": "remote_ip_org",
    "asn": "remote_ip_asn",
    "bogon": "remote_ip_bogon",
}


def geocode_ip(ip_address: str) -> dict:
    """Geocode an ip address using ipinfo's Lite API.

    This function uses joblib to cache api calls so we only have to
    call the api once for a given ip address. The cache location is looked up on each
    call, so it follows ``PUDL_METRICS_LOCAL_DATA_DIR``.

    Args:
        ip_address: An ip address.

    Returns:
        details: Ip location and org information.
    """
    cache = Memory(get_ipinfo_cache_dir(), verbose=0)
    return cache.cache(_geocode_ip)(ip_address)


def _geocode_ip(ip_address: str) -> dict:
    """Call the ipinfo Lite API for an ip address, uncached.

    Args:
        ip_address: An ip address.

    Returns:
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
