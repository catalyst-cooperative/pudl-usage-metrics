"""Extract partitioned data from Github logs.

This includes data which returns a window of results (e.g., the last two weeks) when
querying the Github API.

``scripts/save_github_metrics.py`` archives each response once a day. The popular
paths and referrers are totals over the whole window, so a day's partition is the
archive saved that day. Views and clones are counted per day, so a day's counts appear
in every archive saved over the following two weeks. A day isn't complete in the
archive saved that same day, and GitHub's counts may still grow the day after, so the
partition reads the archives saved from two days after it onwards.
"""

import json
import re
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import ClassVar, Literal, get_args

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    DailyPartitionsDefinition,
    Output,
    asset,
)
from google.cloud import storage

from usage_metrics.paths import PUDL_METRICS_ARCHIVES_BUCKET
from usage_metrics.raw.extract import (
    GCS_EXTRACT_RETRY_POLICY,
    GCSExtractor,
    report_warning,
)

DailyMetricType = Literal["clones", "popular_paths", "popular_referrers", "views"]
CumulativeMetricType = Literal["stargazers", "forks"]
GithubMetricType = DailyMetricType | CumulativeMetricType

DAILY_METRIC_TYPES: list[DailyMetricType] = list(get_args(DailyMetricType))
CUMULATIVE_METRIC_TYPES: list[CumulativeMetricType] = list(
    get_args(CumulativeMetricType)
)
GITHUB_METRIC_TYPES: list[GithubMetricType] = (
    DAILY_METRIC_TYPES + CUMULATIVE_METRIC_TYPES
)

PER_DAY_METRIC_TYPES: list[DailyMetricType] = ["clones", "views"]
"""Metrics whose archives hold a separate count for each of the last two weeks."""

ARCHIVE_AGES = range(2, 16)
"""How many days after a day the archives holding its views and clones are saved."""

FIRST_ARCHIVE_DUE = timedelta(days=ARCHIVE_AGES.start, hours=10)
"""How long after the start of a day the first archive of its views and clones is
saved, with time to spare for a delayed ``save-daily-metrics`` run."""


def _utcnow() -> datetime:
    return datetime.now(UTC)


class GithubExtractor(GCSExtractor):
    """Extractor for Github logs."""

    def __init__(self, metric: GithubMetricType, *args, **kwargs):
        """Initialize the extrator."""
        self.dataset_name = "pudl_github_logs"
        self.bucket_name = PUDL_METRICS_ARCHIVES_BUCKET
        self.metric = metric
        super().__init__(*args, **kwargs)

    def get_blob_prefix(self, context: AssetExecutionContext) -> str:
        """Filter the bucket listing to this metric's blobs server-side."""
        return f"github/{self.metric}/"

    def filter_blobs(
        self, context: AssetExecutionContext, blobs: Iterable[storage.Blob]
    ) -> list[storage.Blob]:
        """From all possible files in a bucket, filter to include relevant ones.

        For the cumulative metrics, grab the most recent file. For views and clones,
        grab the archives that hold the partition's day, and for the popular paths and
        referrers, the archive saved that day.

        Args:
            context: The Dagster asset execution context
            blobs: the list of all file blobs in the bucket, returned by bucket.list_blobs()
            metric: Github metric to apply filtering for.

        Returns:
            A list of blobs to be downloaded.
        """
        if self.metric in DAILY_METRIC_TYPES:
            day = date.fromisoformat(context.partition_key)
            ages = ARCHIVE_AGES if self.metric in PER_DAY_METRIC_TYPES else [0]
            file_names = {
                f"github/{self.metric}/{day + timedelta(days=age)}.json" for age in ages
            }
            filtered_blobs: list[storage.Blob] = [
                blob for blob in blobs if blob.name in file_names
            ]
        else:
            candidate_blobs: list[storage.Blob] = [
                blob
                for blob in blobs
                if blob.name is not None
                and blob.name.startswith(f"github/{self.metric}/")
            ]

            def _time_created(blob: storage.Blob) -> datetime:
                assert blob.time_created is not None, (
                    f"Blob {blob.name} has no time_created; it may not have been "
                    "reloaded from the server."
                )
                return blob.time_created

            filtered_blobs = [max(candidate_blobs, key=_time_created)]

        return filtered_blobs

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

    extract_funcs: ClassVar = {
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
            date_match = re.search(r"\d{4}-\d{2}-\d{2}", str(file_path))
            if date_match is None:
                raise ValueError(f"Could not find a date in file path {file_path}")
            gh_df["metrics_date"] = date_match.group()
        return gh_df


def _extract_day_counts(
    context: AssetExecutionContext, extractor: GithubExtractor
) -> Iterator[Output[pd.DataFrame]]:
    """Yield the partition day's views or clones from every archive that has them.

    GitHub leaves out days without any traffic, so a day that no archive lists produces
    nothing, quietly. So does a day whose first archive isn't due yet. Finding no data
    at all once it is due is reported.
    """
    key = context.partition_key
    frame = extractor.extract(context)
    if frame.empty:
        due = datetime.combine(date.fromisoformat(key), time(0), UTC)
        due += FIRST_ARCHIVE_DUE
        if _utcnow() < due:
            context.log.info(f"Skipping {key}: its archives aren't due until {due}.")
        else:
            report_warning(
                context,
                f"No archived {extractor.metric} for {key}; nothing was written.",
            )
        return
    day_counts = frame.loc[frame.timestamp.str.startswith(key)]
    if day_counts.empty:
        context.log.info(f"GitHub lists no {extractor.metric} for {key}.")
        return
    yield Output(day_counts)


def daily_metrics_extraction_factory(
    metric: DailyMetricType,
) -> AssetsDefinition:
    """Create Dagster asset for each daily-reported metric."""

    @asset(
        name=f"raw_github_{metric}",
        partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
        tags={"source": "github_partitioned"},
        retry_policy=GCS_EXTRACT_RETRY_POLICY,
        output_required=False,
    )
    def _raw_github_logs(
        context: AssetExecutionContext,
    ) -> Iterator[Output[pd.DataFrame]]:
        """Extract the Github metrics of a day from the archived API responses."""
        extractor = GithubExtractor(metric=metric)
        if metric in PER_DAY_METRIC_TYPES:
            yield from _extract_day_counts(context, extractor)
        else:
            yield from extractor.extract_partition(context)

    return _raw_github_logs


raw_github_partitioned_assets = [
    daily_metrics_extraction_factory(metric) for metric in DAILY_METRIC_TYPES
]
