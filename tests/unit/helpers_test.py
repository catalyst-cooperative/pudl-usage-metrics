"""Test util functions."""

import pandas as pd
import pytest

from usage_metrics.helpers import (
    convert_camel_case_columns_to_snake_case,
    geocode_ip,
    geocode_ips,
    parse_request_url,
)


def test_geocode_ip() -> None:
    """Test Google Public DNS IP against the Lite API.

    Unlike the old Core API, Lite doesn't return city/region/loc/postal/timezone
    (which used to make this assertion flaky on lat/long), so this should be
    stable.
    """
    geocoded_ip = geocode_ip("8.8.8.8")
    assert geocoded_ip == {
        "ip": "8.8.8.8",
        "asn": "AS15169",
        "as_name": "Google LLC",
        "as_domain": "google.com",
        "country_code": "US",
        "country": "United States",
        "continent_code": "NA",
        "continent": {
            "code": "NA",
            "name": "North America",
        },
        "country_name": "United States",
        "isEU": False,
        "country_flag_url": (
            "https://cdn.ipinfo.io/static/images/countries-flags/US.svg"
        ),
        "country_flag": {
            "emoji": "🇺🇸",
            "unicode": "U+1F1FA U+1F1F8",
        },
        "country_currency": {
            "code": "USD",
            "symbol": "$",
        },
    }


def test_geocode_ips_maps_lite_fields_and_handles_bogon(monkeypatch) -> None:
    """`geocode_ips` should rename Lite fields and tolerate a bogon's sparse response.

    A bogon IP's response is just ``{"ip": ..., "bogon": True}`` -- no
    asn/country/etc -- so the missing fields should come through as null rather
    than raising.
    """
    fake_responses = {
        "8.8.8.8": {
            "ip": "8.8.8.8",
            "asn": "AS15169",
            "as_name": "Google LLC",
            "country_code": "US",
            "country_name": "United States",
        },
        "10.0.0.1": {"ip": "10.0.0.1", "bogon": True},
    }
    monkeypatch.setattr(
        "usage_metrics.helpers.geocode_ip", lambda ip: fake_responses[ip]
    )

    df = pd.DataFrame({"remote_ip": ["8.8.8.8", "10.0.0.1"]})
    geocoded = geocode_ips(df)

    google_row = geocoded.loc[geocoded.remote_ip == "8.8.8.8"].iloc[0]
    assert google_row.remote_ip_asn == "AS15169"
    assert google_row.remote_ip_org == "Google LLC"
    assert google_row.remote_ip_country == "US"
    assert google_row.remote_ip_country_name == "United States"

    bogon_row = geocoded.loc[geocoded.remote_ip == "10.0.0.1"].iloc[0]
    assert bogon_row.remote_ip_bogon is True
    assert pd.isna(bogon_row.remote_ip_asn)
    assert pd.isna(bogon_row.remote_ip_country)


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
        (pd.DataFrame(columns=["CamelCase"]), pd.DataFrame(columns=["camel_case"])),
        (pd.DataFrame(columns=["Single"]), pd.DataFrame(columns=["single"])),
        (pd.DataFrame(columns=["S"]), pd.DataFrame(columns=["s"])),
    ],
)
def test_convert_camel_case_columns_to_snake_case(camel_case_df, snake_case_df) -> None:
    """Test camel case to snake case."""
    result_df = convert_camel_case_columns_to_snake_case(camel_case_df)
    pd.testing.assert_frame_equal(result_df, snake_case_df)
