"""Transform data from Github logs for stargazer and fork data."""

import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    asset,
)


@asset(
    io_manager_key="database_manager",
    tags={"source": "github_nonpartitioned"},
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
    tags={"source": "github_nonpartitioned"},
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
