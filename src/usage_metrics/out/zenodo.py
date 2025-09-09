"""Create outputs from Zenodo logs."""

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "zenodo"},
)
def out_zenodo_logs(
    context: AssetExecutionContext,
    core_zenodo_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Output daily Zenodo logs.

    Calculate differences from the cumulative views and downloads columns.
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if core_zenodo_logs.empty:
        return core_zenodo_logs
    metrics_cols = [
        "dataset_downloads",
        "dataset_unique_downloads",
        "dataset_views",
        "dataset_unique_views",
        "version_downloads",
        "version_unique_downloads",
        "version_views",
        "version_unique_views",
    ]

    # Drop mistaken/deleted archives
    df = core_zenodo_logs.loc[~core_zenodo_logs.version_id.isin([13919960, 13920120])]

    df["metrics_date"] = pd.to_datetime(df["metrics_date"])
    # First, ffill all gaps in between dates in case any daily downloads failed
    df = df.set_index(["metrics_date", "version_id"]).unstack().ffill()
    # Then, backfill all dates prior to the existence of the datasets with 0 for the metric columns
    idx = pd.IndexSlice
    df.loc(axis=1)[idx[metrics_cols, :]] = (
        df.loc(axis=1)[idx[metrics_cols, :]].fillna(0).astype(int)
    )
    # Backfill the metadata
    df = df.bfill()
    # # Convert the cumulative sum columns to diff columns
    df.loc(axis=1)[idx[metrics_cols, :]] = (
        df.loc(axis=1)[idx[metrics_cols, :]].diff().fillna(0)
    )

    new_df = df.stack()
    # Rename the diff columns
    new_df = new_df.rename({col: "new_" + col for col in metrics_cols}, axis=1)

    return new_df.reset_index()
