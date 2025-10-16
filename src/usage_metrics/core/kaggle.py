"""Transform data from Kaggle logs."""

import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "kaggle"},
)
def core_kaggle_logs(
    context: AssetExecutionContext,
    raw_kaggle_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Transform daily Kaggle logs.

    Add column headers. The Kaggle data contains a series of columns with the suffix
    "{column_name}Nullable" and a series of columns with the prefix
    "has{column_name}". The "Nullable" columns contain the data observed in the
    corresponding columns, but with NAs where the value is not reported. The "has"
    columns are booleans indicating whether or not the data is NA. Given that a column,
    its "has' and "Nullable" report overlapping information, we keep the "Nullable"
    columns and drop the other two.
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_kaggle_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_kaggle_logs

    # Where there is a "nullable" column, drop the corresponding "has" and duplicate column from data
    # This only exists prior to 09/21/2025.
    nullable_cols = [col for col in raw_kaggle_logs.columns if "Nullable" in col]
    duplicated_cols = [col.replace("Nullable", "") for col in nullable_cols] + [
        "has{}".format((col[0].upper() + col[1:]).replace("Nullable", ""))
        for col in nullable_cols
    ]

    df = raw_kaggle_logs.drop(columns=duplicated_cols)
    # Newer data doesn't have the "Nullable" suffix, so we drop this suffix and
    # then perform the rename to ensure consistency over time.
    df.columns = df.columns.str.removesuffix("Nullable")

    df = df.rename(
        columns={
            "datasetSlug": "dataset_name",
            "ownerUser": "owner",
            "usabilityRating": "usability_rating",
            "title": "title",
            "subtitle": "subtitle",
            "description": "description",
            "datasetId": "dataset_id",
            "totalViews": "total_views",
            "totalVotes": "total_votes",
            "totalDownloads": "total_downloads",
            "isPrivate": "is_private",
        }
    )

    # Convert string to datetime using Pandas
    df["metrics_date"] = pd.to_datetime(df["metrics_date"])

    # Check validity of PK column
    df = df.set_index("metrics_date")
    assert df.index.is_unique

    # For now, dump dictionaries and lists into a string
    # We don't need these for metrics and expect them to stay essentially the same over time.
    to_string_cols = [
        col
        for col in ["data", "keywords", "licenses", "collaborators"]
        if col in df.columns
    ]
    df[to_string_cols] = df[to_string_cols].astype(str)

    context.log.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} environment.")

    return df.reset_index()
