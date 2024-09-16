"""Extract data from S3 logs."""

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    WeeklyPartitionsDefinition,
    asset,
)
from google.cloud import storage
from tqdm import tqdm

BUCKET_URI = "pudl-usage-metrics-archives.catalyst.coop"
PATH_EXT = "data/github/"
WEEKLY_METRIC_TYPES = ["clones", "popular_paths", "popular_referrers", "views"]
CUMULATIVE_METRIC_TYPES = ["stargazers", "forks"]
GITHUB_METRIC_TYPES = WEEKLY_METRIC_TYPES + CUMULATIVE_METRIC_TYPES


def extract_clones(metric_json):
    """Extract clone data from clone JSON file."""
    return pd.DataFrame(metric_json["clones"])


def extract_views(metric_json):
    """Extract views data from views JSON file."""
    return pd.DataFrame(metric_json["views"])


def extract_popular_paths(metric_json):
    """Extract popular paths data from popular paths JSON file."""
    return pd.DataFrame(metric_json)


def extract_popular_referrers(metric_json):
    """Extract popular referrers data from popular referrers JSON file."""
    return pd.DataFrame(metric_json)


def extract_stargazers(metric_json):
    """Extract stargazers data from stargazers JSON file."""
    trns_metric_json = []
    for stargazer in metric_json:
        user = stargazer["user"]
        starred_at = stargazer["starred_at"]
        user["starred_at"] = starred_at
        trns_metric_json.append(user)

    metric_df = pd.DataFrame(trns_metric_json)
    metric_df["starred_at"] = pd.to_datetime(metric_df["starred_at"])
    metric_df = metric_df.set_index("starred_at")
    return metric_df


def extract_forks(metric_json):
    """Extract forks data from forks JSON file."""
    metric_df = pd.DataFrame(metric_json)
    metric_df["created_at"] = pd.to_datetime(metric_df["created_at"])
    metric_df = metric_df.set_index("created_at")
    return metric_df


extract_funcs = {
    "clones": extract_clones,
    "views": extract_views,
    "popular_paths": extract_popular_paths,
    "popular_referrers": extract_popular_referrers,
    "stargazers": extract_stargazers,
    "forks": extract_forks,
}


def list_blobs_within_folder(bucket_name, folder):
    """Gets blobs within a bucket folder."""
    storage_client = storage.Client()

    blobs = list(storage_client.list_blobs(bucket_name, prefix=folder))
    blobs = list(filter(lambda x: x.name != folder, blobs))
    return blobs


def extract_data(path: Path, metric: Literal[*GITHUB_METRIC_TYPES]):
    """Gets a dataframe of the most recent persistent metric data."""
    with Path.open(path) as metric_file:
        file_contents = metric_file.read()
    metric_json = json.loads(file_contents)
    extract_func = extract_funcs[metric]
    gh_df = extract_func(metric_json)
    return gh_df


def get_weekly_files_from_folder(
    blobs: storage.Blob, partition_dates: tuple[str]
) -> list[storage.Blob]:
    """Get relevant weekly metric blobs from a GCS folder."""
    return [
        blob for blob in blobs if any(date in blob.name for date in partition_dates)
    ]


def get_cumulative_file_from_folder(blobs: storage.Blob) -> list[storage.Blob]:
    """Get relevant cumulative metric blobs from a GCS folder."""
    return [sorted(blobs, key=lambda x: x.time_created)[-1]]


def download_github_logs_from_blobs(
    blobs: list[storage.Blob],
    download_dir: Path,
    metric: Literal[*WEEKLY_METRIC_TYPES],
) -> list[Path]:
    """Given a list of blobs, download them locally.

    If the file already exists locally don't download it.
    """
    file_paths = []
    for blob in tqdm(blobs):
        file_name = metric + "-" + blob.name.split("/")[-1]
        path_to_file = Path(download_dir, file_name)
        if not Path.exists(path_to_file):
            blob.download_to_filename(path_to_file)
            if Path.stat(path_to_file).st_size == 0:
                # Handle download interruptions. #TODO: Less janky way to do this?
                blob.download_to_filename(Path(download_dir, file_name))

        file_paths.append(Path(download_dir, file_name))

    return file_paths


def weekly_metrics_extraction_factory(
    metric: Literal[*WEEKLY_METRIC_TYPES],
) -> AssetsDefinition:
    """Create Dagster asset for each weekly-reported metric."""

    @asset(
        name=f"raw_github_{metric}",
        partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
        tags={"source": "github"},
    )
    def _raw_github_logs(context: AssetExecutionContext) -> pd.DataFrame:
        """Extract Github logs from daily files and return one weekly DataFrame."""
        week_start_date_str = context.partition_key
        week_date_range = pd.date_range(start=week_start_date_str, periods=7, freq="D")

        weekly_dfs = []

        with TemporaryDirectory() as td:
            # Determine where to save these files
            if os.environ.get("DATA_DIR"):
                download_dir = Path(os.environ.get("DATA_DIR"), "github/")
                if not Path.exists(download_dir):
                    Path.mkdir(download_dir)
            else:
                download_dir = td
            context.log.info(f"Saving Github logs to {download_dir}.")

            # Get all logs in a week
            blobs = list_blobs_within_folder(BUCKET_URI, "github/" + metric + "/")
            blobs = get_weekly_files_from_folder(
                blobs, partition_dates=tuple(week_date_range.strftime("%Y-%m-%d"))
            )

            file_paths = download_github_logs_from_blobs(
                blobs=blobs, download_dir=download_dir, metric=metric
            )

            for path in file_paths:
                try:
                    # TODO - add time of blob creation to popular paths and referrers!!!
                    weekly_dfs.append(extract_data(path=path, metric=metric))
                except pd.errors.EmptyDataError:
                    context.log.warnings(f"{path} is an empty file, couldn't read.")
            if weekly_dfs:  # If data
                return pd.concat(weekly_dfs)
            return pd.DataFrame()

    return _raw_github_logs


def cumulative_metrics_extraction_factory(
    metric: Literal[*CUMULATIVE_METRIC_TYPES],
) -> AssetsDefinition:
    """Create Dagster asset for each cumulatively-reported metric."""

    @asset(name=f"raw_github_{metric}", tags={"source": "github"})
    def _raw_github_logs(context: AssetExecutionContext) -> pd.DataFrame:
        """Extract Github logs from daily files and return one weekly DataFrame."""
        with TemporaryDirectory() as td:
            # Determine where to save these files
            if os.environ.get("DATA_DIR"):
                download_dir = Path(os.environ.get("DATA_DIR"), "github/")
                if not Path.exists(download_dir):
                    Path.mkdir(download_dir)
            else:
                download_dir = td
            context.log.info(f"Saving Github logs to {download_dir}.")

            # Get all logs in a week
            blobs = list_blobs_within_folder(BUCKET_URI, "github/" + metric + "/")
            blobs = get_cumulative_file_from_folder(blobs)

            file_paths = download_github_logs_from_blobs(
                blobs=blobs, download_dir=download_dir, metric=metric
            )

            assert len(file_paths) == 1
            try:
                return extract_data(path=file_paths[0], metric=metric)
            except pd.errors.EmptyDataError:
                context.log.warnings(
                    f"{file_paths[0]} is an empty file, couldn't read."
                )
                return pd.DataFrame()

    return _raw_github_logs


raw_github_assets = [
    weekly_metrics_extraction_factory(metric) for metric in WEEKLY_METRIC_TYPES
] + [
    cumulative_metrics_extraction_factory(metric) for metric in CUMULATIVE_METRIC_TYPES
]
