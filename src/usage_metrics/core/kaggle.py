"""Transform data from Kaggle logs."""

import pandas as pd
from dagster import (
    asset,
)


@asset()
def transform_kaggle_logs(extract_kaggle_logs: pd.DataFrame) -> pd.DataFrame:
    """Transform Kaggle usage metrics."""
    # Drop all columns that aren't related to usage metrics
    extract_kaggle_logs = extract_kaggle_logs[
        [
            "date_time",
            "info.usabilityRatingNullable",
            "info.totalViews",
            "info.totalVotes",
            "info.totalDownloads",
        ]
    ]
    extract_kaggle_logs.columns = [
        "time",
        "usability_rating",
        "total_views",
        "total_votes",
        "total_downloads",
    ]
    return extract_kaggle_logs
