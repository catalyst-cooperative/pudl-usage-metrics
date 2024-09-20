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
    tags={"source": "github"},
)
def core_github_popular_referrers(
    context: AssetExecutionContext,
    raw_github_popular_referrers: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the popular referrers to the PUDL Github repository."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_github_popular_referrers.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_github_popular_referrers

    df = raw_github_popular_referrers
    df = df.rename(columns={"count": "total_referrals", "uniques": "unique_referrals"})

    # Convert string to datetime using Pandas
    df["metrics_date"] = pd.to_datetime(df["metrics_date"])

    # Check validity of PK column
    df = df.set_index(["metrics_date", "referrer"])
    assert df.index.is_unique

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "github"},
)
def core_github_popular_paths(
    context: AssetExecutionContext,
    raw_github_popular_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the popular paths to the PUDL Github repository."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_github_popular_paths.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_github_popular_paths

    df = raw_github_popular_paths
    df = df.rename(columns={"count": "total_views", "uniques": "unique_views"})

    # Convert string to datetime using Pandas
    df["metrics_date"] = pd.to_datetime(df["metrics_date"])

    # Check validity of PK column
    df = df.set_index(["metrics_date", "path"])
    assert df.index.is_unique

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "github"},
)
def core_github_clones(
    context: AssetExecutionContext,
    raw_github_clones: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the clones to the PUDL Github repository.

    The raw data includes clones from a two week window. To avoid duplication between
    partitions, we filter the records here by the time window in the partition key.
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_github_clones.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_github_clones

    df = raw_github_clones
    df = df.rename(
        columns={
            "count": "total_clones",
            "uniques": "unique_clones",
            "timestamp": "metrics_date",
        }
    )

    # Drop any repeated timestamps between snapshots
    df = df.drop_duplicates("metrics_date")
    df["metrics_date"] = pd.to_datetime(df["metrics_date"])

    # Only keep records within the week covered by the partition key to avoid duplicated
    # values between partitions
    week_start_date_str = context.partition_key
    week_date_range = pd.date_range(start=week_start_date_str, periods=7, freq="D")
    df = df.loc[
        df.metrics_date.between(
            week_date_range.min().strftime("%Y-%m-%d"),
            week_date_range.max().strftime("%Y-%m-%d"),
        )
    ]

    # Check validity of PK column
    df = df.set_index("metrics_date")
    assert df.index.is_unique

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "github"},
)
def core_github_views(
    context: AssetExecutionContext,
    raw_github_views: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the views to the PUDL Github repository.

    The raw data includes views from a two week window. To avoid duplication between
    partitions, we filter the records here by the time window in the partition key.
    """
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_github_views.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_github_views

    df = raw_github_views
    df = df.rename(
        columns={
            "count": "total_views",
            "uniques": "unique_views",
            "timestamp": "metrics_date",
        }
    )

    # Drop any repeated timestamps between snapshots
    df = df.drop_duplicates("metrics_date")
    df["metrics_date"] = pd.to_datetime(df["metrics_date"])

    # Only keep records within the week covered by the partition key to avoid duplicated
    # values between partitions
    week_start_date_str = context.partition_key
    week_date_range = pd.date_range(start=week_start_date_str, periods=7, freq="D")
    df = df.loc[
        df.metrics_date.between(
            week_date_range.min().strftime("%Y-%m-%d"),
            week_date_range.max().strftime("%Y-%m-%d"),
        )
    ]

    # Check validity of PK column
    df = df.set_index("metrics_date")
    assert df.index.is_unique

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()


@asset(
    io_manager_key="database_manager",
    tags={"source": "github"},
)
def core_github_forks(
    context: AssetExecutionContext,
    raw_github_forks: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the forks to the PUDL Github repository."""
    df = raw_github_forks

    # Drop 'fork' column and all URLs other than the main one for the repository
    # Also drop "open_issues_count" which is identical to "open_issues"
    df = df.rename(columns={"size": "size_kb"}).drop(
        columns=[
            col
            for col in df.columns
            if "_url" in col or col == "fork" or col == "open_issues_count"
        ]
    )

    # Convert string to datetime using Pandas
    df[["created_at", "updated_at", "pushed_at"]] = df[
        ["created_at", "updated_at", "pushed_at"]
    ].apply(pd.to_datetime)

    # Check validity of PK column
    df = df.set_index("id")
    assert df.index.is_unique

    # For now, dump dictionaries and lists into a string
    # We don't need these for metrics and expect them to stay essentially the same over time.
    df[["owner", "permissions", "license", "topics"]] = df[
        ["owner", "permissions", "license", "topics"]
    ].astype(str)

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()


@asset(
    io_manager_key="database_manager",
    tags={"source": "github"},
)
def core_github_stargazers(
    context: AssetExecutionContext,
    raw_github_stargazers: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the stargazers to the PUDL Github repository."""
    df = raw_github_stargazers

    # Drop 'fork' column and all URLs other than the main one for the repository
    # Also drop "open_issues_count" which is identical to "open_issues"
    df = df.rename({"size": "size_kb"}).drop(columns=["gravatar_id", "avatar_url"])

    # Convert string to datetime using Pandas
    df["starred_at"] = pd.to_datetime(df["starred_at"])

    # Check validity of PK column
    df = df.set_index("id")
    assert df.index.is_unique

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()