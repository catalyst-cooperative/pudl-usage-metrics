import os
import ipinfo

from pathlib import Path
from urllib.parse import urlparse
from joblib import Memory

cache_dir = Path("/app/cache")
ip_address_cache = Memory(cache_dir, verbose=0)

@ip_address_cache.cache
def geocode_ip(ip_address):
    """Geocode an ip address using ipinfo API."""
    IPINFO_TOKEN = os.environ["IPINFO_TOKEN"]
    handler = ipinfo.getHandler(IPINFO_TOKEN)

    details = handler.getDetails(ip_address)
    return details.all

def parse_request_url(url):
    """Create dictionary of request components."""
    pr = urlparse(url)
    return {"scheme": pr.scheme, "netloc": pr.netloc, "path": pr.path, "query": pr.query}