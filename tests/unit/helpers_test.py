"""Test util functions."""
from usage_metrics.helpers import geocode_ip, parse_request_url


def test_geocode_ip() -> None:
    """Test Google Public DNS IP."""
    geocoded_ip = geocode_ip("8.8.8.8")
    assert geocoded_ip == {
        "ip": "8.8.8.8",
        "hostname": "dns.google",
        "anycast": True,
        "city": "Mountain View",
        "region": "California",
        "country": "US",
        "loc": "37.4056,-122.0775",
        "org": "AS15169 Google LLC",
        "postal": "94043",
        "timezone": "America/Los_Angeles",
        "country_name": "United States",
        "latitude": "37.4056",
        "longitude": "-122.0775",
    }


def test_url_parse() -> None:
    """Test url parsing."""
    url = "https://data.catalyst.coop/ferc1/f1_cash_flow"
    parsed_url = parse_request_url(url)

    assert parsed_url == {
        "scheme": "https",
        "netloc": "data.catalyst.coop",
        "path": "/ferc1/f1_cash_flow",
        "query": "",
    }
