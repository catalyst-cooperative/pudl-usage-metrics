"""General utility functions for cleaning usage metrics data."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import ipinfo
import pandas as pd
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


def convert_camel_case_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert CamelCase columns of a dataframe to snake_case.

    Args:
        df: A dataframe with CamelCase columns.
    Returns:
        df: A dataframe with snake_case columns.
    """
    df.columns = df.columns.str.replace(r"(?<!^)(?=[A-Z])", "_").str.lower()
    return df


def unpack_json_series(series: pd.Series) -> pd.DataFrame:
    """
    Unpack a series containing json records to a DataFrame.

    Expects no more than one json record per series element.

    Args:
        series: A pandas series on json records.
    Returns:
        unpacked_df: A dataframe where columns are the fields of the json records.
    """
    series_dict = series.to_dict()
    # Replace missing data with empty dicts
    series_dict = {index: v if v else {} for index, v in series_dict.items()}

    unpacked_df = pd.DataFrame.from_dict(series_dict, orient="index")
    assert len(unpacked_df) <= len(series)
    return unpacked_df


def str_to_datetime(
    date: str, fmt: str = "%Y-%m-%d", tzinfo: timezone = timezone.utc
) -> datetime:
    """Convert a string to a date."""
    return datetime.strptime(date, fmt).replace(tzinfo=tzinfo)
