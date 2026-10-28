"""Test util functions."""

import pandas as pd
import pytest

from usage_metrics.helpers import (
    geocode_ip,
    geocode_ips,
)

# What ipinfo's Lite API (plus the client's country_name lookup) returns for
# Google Public DNS. Only fields the pipeline uses or the API itself provides;
# the client also adds flag/currency/isEU/etc., which we deliberately ignore.
GOOGLE_DNS = {
    "ip": "8.8.8.8",
    "asn": "AS15169",
    "as_name": "Google LLC",
    "as_domain": "google.com",
    "country_code": "US",
    "country_name": "United States",
    "continent_code": "NA",
}


def test_geocode_ip() -> None:
    """Test Google Public DNS IP against the Lite API."""
    geocoded_ip = geocode_ip(GOOGLE_DNS["ip"])
    assert {k: geocoded_ip[k] for k in GOOGLE_DNS} == GOOGLE_DNS


@pytest.mark.parametrize(
    "ip,expected",
    [
        (
            GOOGLE_DNS["ip"],
            {
                "remote_ip_asn": GOOGLE_DNS["asn"],
                "remote_ip_org": GOOGLE_DNS["as_name"],
                "remote_ip_country": GOOGLE_DNS["country_code"],
                "remote_ip_country_name": GOOGLE_DNS["country_name"],
            },
        ),
        # A bogon's response is just {"ip": ..., "bogon": True}, so every other
        # column must come through as null rather than raising a KeyError.
        (
            "10.0.0.1",
            {
                "remote_ip_bogon": True,
                "remote_ip_asn": None,
                "remote_ip_org": None,
                "remote_ip_country": None,
                "remote_ip_country_name": None,
            },
        ),
    ],
)
def test_geocode_ips(ip: str, expected: dict) -> None:
    """`geocode_ips` should map real ipinfo responses onto usage_metrics columns."""
    row = geocode_ips(pd.DataFrame({"remote_ip": [ip]})).iloc[0]
    for column, value in expected.items():
        if value is None:
            assert pd.isna(row[column]), column
        else:
            assert row[column] == value, column


def test_geocode_ips_mixed_batch_with_duplicates() -> None:
    """Duplicate IPs are geocoded once and merged back onto every log row."""
    ips = [GOOGLE_DNS["ip"], "10.0.0.1", GOOGLE_DNS["ip"]]
    geocoded = geocode_ips(pd.DataFrame({"remote_ip": ips}))

    assert geocoded.remote_ip.tolist() == ips
    assert geocoded.remote_ip_country.tolist()[::2] == [GOOGLE_DNS["country_code"]] * 2
    assert pd.isna(geocoded.remote_ip_country.iloc[1])
