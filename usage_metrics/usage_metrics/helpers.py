"""General utility functions for cleaning usage metrics data."""
import os
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

import ipinfo
from joblib import Memory

cache_dir = Path(__file__).parent / "cache"
ip_address_cache = Memory(cache_dir, verbose=0)


@ip_address_cache.cache
def geocode_ip(ip_address: str) -> Dict:
    """Geocode an ip address using ipinfo API."""
    try:
        IPINFO_TOKEN = os.environ["IPINFO_TOKEN"]
    except KeyError:
        raise AssertionError("Can't find IPINFO_TOKEN.")
    handler = ipinfo.getHandler(IPINFO_TOKEN)

    details = handler.getDetails(ip_address)
    return details.all


def parse_request_url(url: str) -> Dict:
    """Create dictionary of request components."""
    pr = urlparse(url)
    return {
        "scheme": pr.scheme,
        "netloc": pr.netloc,
        "path": pr.path,
        "query": pr.query,
    }
