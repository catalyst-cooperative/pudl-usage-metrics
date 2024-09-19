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
    """Transform daily S3 logs.

    Add column headers, geocode values,
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_kaggle_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_kaggle_logs

    # Where there is a "nullable" column, drop the corresponding "has" and duplicate column from data
    nullable_cols = [col for col in raw_kaggle_logs.columns if "Nullable" in col]
    duplicated_cols = [col.replace("Nullable", "") for col in nullable_cols] + [
        "has{}".format((col[0].upper() + col[1:]).replace("Nullable", ""))
        for col in nullable_cols
    ]

    df = raw_kaggle_logs.drop(columns=duplicated_cols)
    df = df.rename(
        columns={
            "datasetSlugNullable": "dataset_name",
            "ownerUserNullable": "owner",
            "usabilityRatingNullable": "usability_rating",
            "titleNullable": "title",
            "subtitleNullable": "subtitle",
            "descriptionNullable": "description",
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
    df[["data", "keywords", "licenses", "collaborators"]] = df[
        ["data", "keywords", "licenses", "collaborators"]
    ].astype(str)

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()
