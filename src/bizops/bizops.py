"""Tools for extracting and organizing data about Catalyst's business operations."""

import requests
import os
import pandas as pd

# Authentication required to access Harvest's API
HARVEST_ACCOUNT_ID = os.environ["HARVEST_ACCOUNT_ID"]
HARVEST_TOKEN = os.environ["HARVEST_TOKEN"]
HARVEST_BASE_URL = "https://api.harvestapp.com/api/v2/"

VALID_HARVEST_ENDPOINTS = [
    "time_entries",
    "projects",
    "clients",
]


def get_harvest_df(endpoint):
    """Pull historical data from a Harvest API endpoint and return as a DataFrame."""
    url = f"{HARVEST_BASE_URL}/{endpoint}"
    headers = {
        "Harvest-Account-ID": f"{HARVEST_ACCOUNT_ID}",
        "Authorization": f"Bearer {HARVEST_TOKEN}",
        "User-Agent": "Project Management GitHub Workflow (zane.selvans@catalyst.coop)",
    }
    # Build up a dataframe containing all of the time entries from Harvest:
    out_df = pd.DataFrame()
    page = 1
    while page:
        response_json = requests.get(
            url=url,
            headers=headers,
            params={"page": page},
        ).json()
        new_df = pd.json_normalize(response_json[endpoint])
        out_df = out_df.append(new_df)
        page = response_json["next_page"]
    return out_df
