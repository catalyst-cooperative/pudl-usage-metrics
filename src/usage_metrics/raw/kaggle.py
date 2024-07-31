"""Extract data from Kaggle logs."""

import pandas as pd
from dagster import (
    asset,
)
from kaggle.api.kaggle_api_extended import KaggleApi

KAGGLE_OWNER = "catalystcooperative"
KAGGLE_DATASET = "pudl-project"


@asset()
def extract_kaggle_logs() -> pd.DataFrame:
    """Download PUDL project usage metadata from Kaggle site."""
    api = KaggleApi()

    metadata = api.metadata_get(KAGGLE_OWNER, KAGGLE_DATASET)
    metadata_df = pd.json_normalize(metadata)
    metadata_df["date_time"] = pd.Timestamp("now").date()
    return metadata_df
