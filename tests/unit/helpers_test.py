"""Test util functions."""

import pandas as pd
import pytest
from usage_metrics.helpers import (
    convert_camel_case_columns_to_snake_case,
    geocode_ip,
    parse_request_url,
)


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
        "continent": {
            "code": "NA",
            "name": "North America",
        },
        "country_currency": {
            "code": "USD",
            "symbol": "$",
        },
        "country_flag": {
            "emoji": "🇺🇸",
            "unicode": "U+1F1FA U+1F1F8",
        },
        "country_flag_url": (
            "https://cdn.ipinfo.io/static/images/countries-flags/US.svg"
        ),
        "loc": "37.4056,-122.0775",
        "org": "AS15169 Google LLC",
        "postal": "94043",
        "timezone": "America/Los_Angeles",
        "country_name": "United States",
        "latitude": "37.4056",
        "longitude": "-122.0775",
        "isEU": False,
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


@pytest.mark.parametrize(
    "camel_case_df,snake_case_df",
    [
        (pd.DataFrame(columns=["CamelCase"]), pd.DataFrame(columns=["camelcase"])),
        (pd.DataFrame(columns=["Single"]), pd.DataFrame(columns=["single"])),
        (pd.DataFrame(columns=["S"]), pd.DataFrame(columns=["s"])),
    ],
)
def test_convert_camel_case_columns_to_snake_case(camel_case_df, snake_case_df) -> None:
    """Test camel case to snake case."""
    result_df = convert_camel_case_columns_to_snake_case(camel_case_df)
    pd.testing.assert_frame_equal(result_df, snake_case_df)
