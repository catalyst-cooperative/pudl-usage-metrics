from urllib.parse import urlparse

def parse_request_url(url):
    """Create dictionary of request components."""
    pr = urlparse(url)
    return {"scheme": pr.scheme, "netloc": pr.netloc, "path": pr.path, "params": pr.params, "query": pr.query}