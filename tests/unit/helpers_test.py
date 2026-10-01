"""Test util functions."""

import pandas as pd

from usage_metrics.helpers import (
    geocode_ip,
    geocode_ips,
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
