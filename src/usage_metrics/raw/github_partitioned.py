"""Extract partitioned data from Github logs.

This includes data which returns a window of results (e.g., the last two weeks) when
querying the Github API.
"""

import json
import re
from pathlib import Path
from typing import Literal

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    WeeklyPartitionsDefinition,
    asset,
)
from google.api_core.page_iterator import HTTPIterator
from google.cloud import storage

from usage_metrics.raw.extract import GCSExtractor

WEEKLY_METRIC_TYPES = ["clones", "popular_paths", "popular_referrers", "views"]
CUMULATIVE_METRIC_TYPES = ["stargazers", "forks"]
GITHUB_METRIC_TYPES = WEEKLY_METRIC_TYPES + CUMULATIVE_METRIC_TYPES


class GithubExtractor(GCSExtractor):
    """Extractor for Github logs."""

    def __init__(self, metric: Literal[*GITHUB_METRIC_TYPES], *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.dataset_name = "pudl_github_logs"
        self.bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
        self.metric = metric
        super().__init__(*args, **kwargs)

    def filter_blobs(
        self, context: AssetExecutionContext, blobs: HTTPIterator
    ) -> list[storage.Blob]:
        """From all possible files in a bucket, filter to include relevant ones.

        For the cumulative metrics, grab the most recent file. For the weekly metrics,
        grab all within the matching date range.

        Args:
            context: The Dagster asset execution context
            blobs: the list of all file blobs in the bucket, returned by bucket.list_blobs()
            metric: Github metric to apply filtering for.

        Returns:
            A list of blobs to be downloaded.
        """
        if self.metric in WEEKLY_METRIC_TYPES:
            week_start_date_str = context.partition_key
            week_date_range = pd.date_range(
                start=week_start_date_str, periods=7, freq="D"
            )
            partition_dates = tuple(week_date_range.strftime("%Y-%m-%d"))
            file_name_prefixes = tuple(
                f"github/{self.metric}/{date}.json" for date in partition_dates
            )
            blobs = [blob for blob in blobs if blob.name in file_name_prefixes]
        else:
            blobs = [
                blob for blob in blobs if blob.name.startswith(f"github/{self.metric}/")
            ]
            blobs = [sorted(blobs, key=lambda x: x.time_created)[-1]]

        return blobs

    def extract_clones(self, metric_json):
        """Extract clone data from clone JSON file."""
        return pd.DataFrame(metric_json["clones"])

    def extract_views(self, metric_json):
        """Extract views data from views JSON file."""
        return pd.DataFrame(metric_json["views"])

    def extract_popular_paths(self, metric_json):
        """Extract popular paths data from popular paths JSON file."""
        return pd.DataFrame(metric_json)

    def extract_popular_referrers(self, metric_json):
        """Extract popular referrers data from popular referrers JSON file."""
        return pd.DataFrame(metric_json)

    def extract_stargazers(self, metric_json):
        """Extract stargazers data from stargazers JSON file."""
        trns_metric_json = []
        for stargazer in metric_json:
            user = stargazer["user"]
            starred_at = stargazer["starred_at"]
            user["starred_at"] = starred_at
            trns_metric_json.append(user)

        return pd.DataFrame(trns_metric_json)

    def extract_forks(self, metric_json):
        """Extract forks data from forks JSON file."""
        return pd.DataFrame(metric_json)

    extract_funcs = {
        "clones": extract_clones,
        "views": extract_views,
        "popular_paths": extract_popular_paths,
        "popular_referrers": extract_popular_referrers,
        "stargazers": extract_stargazers,
        "forks": extract_forks,
    }

    def load_file(self, file_path: Path):
        """Gets a dataframe of the most recent persistent metric data."""
        with Path.open(file_path) as metric_file:
            file_contents = metric_file.read()
        metric_json = json.loads(file_contents)
        extract_func = self.extract_funcs[self.metric]
        gh_df = extract_func(self, metric_json)
        # Add date of file as column if the extract combines multiple dataframes
        # and contains no timestamp column
        if self.metric in ["popular_paths", "popular_referrers"]:
            gh_df["metrics_date"] = re.search(
                r"\d{4}-\d{2}-\d{2}", str(file_path)
            ).group()
        return gh_df


def weekly_metrics_extraction_factory(
    metric: Literal[*WEEKLY_METRIC_TYPES],
) -> AssetsDefinition:
    """Create Dagster asset for each weekly-reported metric."""

    @asset(
        name=f"raw_github_{metric}",
        partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
        tags={"source": "github_partitioned"},
    )
    def _raw_github_logs(context: AssetExecutionContext) -> pd.DataFrame:
        """Extract Github logs from daily files and return one weekly DataFrame."""
        return GithubExtractor(metric=metric).extract(context)

    return _raw_github_logs


raw_github_partitioned_assets = [
    weekly_metrics_extraction_factory(metric) for metric in WEEKLY_METRIC_TYPES
]
