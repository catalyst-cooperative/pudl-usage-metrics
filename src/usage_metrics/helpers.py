"""General utility functions for cleaning usage metrics data."""
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import ipinfo
from joblib import Memory

cache_dir = Path(__file__).parents[2] / "cache"
cache_dir.mkdir(exist_ok=True)
ip_address_cache = Memory(cache_dir, verbose=0)

REQUEST_TIMEOUT = 10


@ip_address_cache.cache
def geocode_ip(ip_address: str) -> dict:
    """
    Geocode an ip address using ipinfo API.

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


def parse_request_url(url: str) -> dict:
    """
    Create dictionary of request components.

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
