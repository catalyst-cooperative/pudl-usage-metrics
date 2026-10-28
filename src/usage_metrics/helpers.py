"""General utility functions for cleaning usage metrics data."""

import os
import time
from collections.abc import Callable
from functools import wraps

import ipinfo
import ipinfo.error
import pandas as pd
import requests
from dagster import Backoff, Jitter, OutputContext, RetryPolicy, get_dagster_logger, op
from joblib import Memory

from usage_metrics.paths import get_ipinfo_cache_dir

REQUEST_TIMEOUT = 10

GEOCODE_OP_RETRY_POLICY = RetryPolicy(
    max_retries=5,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)
"""Retry the whole geocoding op, backing off so retries don't all land at once.

Geocoded ips are cached, so a retry only repeats the lookups that hadn't succeeded."""

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


def retry_request(
    retries: int = 3,
    delay: int = 2,
    backoff: int = 2,
    retry_on: tuple[type[Exception], ...] = (requests.exceptions.RequestException,),
    should_retry: Callable[[Exception], bool] | None = None,
):
    """Define a decorator to retry requests with an exponential backoff.

    The first backoff will be ``delay`` seconds, the second ``delay * backoff`` and
    so-on. Exceptions that aren't retried are raised immediately.

    Args:
        retries: how many attempts to make before giving up.
        delay: original number of seconds to wait before retrying.
        backoff: the factor by which to increase the backoff each time.
        retry_on: exception types that may be retried.
        should_retry: optional test applied to a caught exception of a ``retry_on``
            type, returning False if it isn't transient and so shouldn't be retried.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wait = delay
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    if should_retry is not None and not should_retry(e):
                        raise
                    if attempt == retries:
                        raise RuntimeError("Max retries reached") from e
                    time.sleep(wait)
                    wait *= backoff  # Exponential backoff
            raise RuntimeError("Max retries reached")  # Only if retries < 1

        return wrapper

    return decorator


def _is_transient_ipinfo_error(error: Exception) -> bool:
    """Whether an ipinfo failure is worth retrying: network trouble or a 5xx."""
    if isinstance(error, ipinfo.error.APIError):
        return error.error_code >= 500
    return True


_retry_ipinfo = retry_request(
    retries=5,
    delay=1,
    backoff=2,
    retry_on=(requests.exceptions.RequestException, ipinfo.error.APIError),
    should_retry=_is_transient_ipinfo_error,
)
"""Retry transient ipinfo failures: timeouts, connection errors and 5xx responses."""


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
    # Retry outside the cache so a failed call is retried rather than cached.
    return _retry_ipinfo(cache.cache(_geocode_ip))(ip_address)


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


@op(retry_policy=GEOCODE_OP_RETRY_POLICY)
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
    logger = get_dagster_logger()
    logger.info(f"Geocoding {len(unique_ips):,} unique ips from {len(df):,} log rows.")
    start = time.monotonic()
    geocoded_ips = unique_ips.apply(lambda ip: geocode_ip(ip))
    elapsed = time.monotonic() - start
    logger.info(
        f"Geocoded {len(unique_ips):,} unique ips in {elapsed:,.1f} s "
        f"({elapsed / max(len(unique_ips), 1) * 1000:,.1f} ms per ip, including cache hits)."
    )
    geocoded_ips = pd.DataFrame.from_dict(geocoded_ips.to_dict(), orient="index")

    # Keep only the fields the Lite API actually provides (reindex adds any
    # that are missing -- e.g. a bogon IP's response has no asn/country -- as
    # NaN, instead of raising a KeyError) and rename them to their usage_metrics
    # column names.
    geocoded_ips = geocoded_ips.reindex(columns=list(_IPINFO_FIELD_MAP.keys()))
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
    return "-".join(context.get_identifier())
