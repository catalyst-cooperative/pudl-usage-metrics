"""Tests for the partitioned GitHub metrics extractor."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import cast

import pandas as pd
import pytest
from dagster import Output, build_asset_context
from google.cloud import storage

from usage_metrics.core.github_partitioned import core_github_views
from usage_metrics.raw import github_partitioned
from usage_metrics.raw.github_partitioned import (
    GithubExtractor,
    daily_metrics_extraction_factory,
)

DAY = "2024-06-15"


def test_filter_blobs_daily_metric_matches_exact_file(filtered_blob_names):
    """A snapshot metric keeps only ``github/<metric>/<date>.json``."""
    kept = filtered_blob_names(
        GithubExtractor,
        "2024-06-15",
        [
            "github/popular_paths/2024-06-15.json",
            "github/popular_paths/2024-06-16.json",
            "github/popular_referrers/2024-06-15.json",
        ],
        metric="popular_paths",
    )
    assert kept == ["github/popular_paths/2024-06-15.json"]


def test_filter_blobs_cumulative_metric_takes_newest(partition_context, fake_blob):
    """A cumulative metric keeps only the most recently created file."""
    blobs = [
        fake_blob("github/stargazers/2024-01-01.json", b"[]", time_created=1),
        fake_blob("github/stargazers/2024-03-01.json", b"[]", time_created=3),
        fake_blob("github/stargazers/2024-02-01.json", b"[]", time_created=2),
    ]
    ext = GithubExtractor(metric="stargazers")
    kept = ext.filter_blobs(
        partition_context("2024-06-15"), cast(list[storage.Blob], blobs)
    )
    assert [blob.name for blob in kept] == ["github/stargazers/2024-03-01.json"]


def test_load_file_clones(tmp_path):
    """clones data is read straight out of the ``clones`` key."""
    path = tmp_path / "2024-06-15.json"
    path.write_text(
        json.dumps({"clones": [{"timestamp": "2024-06-15T00:00:00Z", "count": 5}]})
    )
    df = GithubExtractor(metric="clones").load_file(path)
    assert df.loc[0, "count"] == 5


def test_load_file_popular_paths_adds_metrics_date(tmp_path):
    """popular_paths rows are dated from the file name."""
    path = tmp_path / "github-popular_paths-2024-06-15.json"
    path.write_text(json.dumps([{"path": "/pudl", "count": 9}]))
    df = GithubExtractor(metric="popular_paths").load_file(path)
    assert df.loc[0, "metrics_date"] == "2024-06-15"


@pytest.fixture
def run_raw_views(fake_gcs, patch_download_many, data_dir, mocker):
    """Run ``raw_github_views`` for ``DAY`` on archives given as
    ``{date saved: {day listed: count}}``. Returns its outputs and the number of
    warnings.
    """

    def _run(archives: dict[str, dict[str, int]], now=datetime(2024, 8, 1, tzinfo=UTC)):
        blobs = {
            f"github/views/{saved}.json": json.dumps(
                {
                    "views": [
                        {"timestamp": f"{day}T00:00:00Z", "count": n, "uniques": n}
                        for day, n in counts.items()
                    ]
                }
            ).encode()
            for saved, counts in archives.items()
        }
        client = fake_gcs({GithubExtractor(metric="views").bucket_name: blobs})
        mocker.patch.object(
            github_partitioned,
            "GithubExtractor",
            side_effect=lambda metric: GithubExtractor(metric=metric, client=client),
        )
        mocker.patch.object(
            github_partitioned, "_utcnow", autospec=True, return_value=now
        )
        report = mocker.patch.object(
            github_partitioned, "report_warning", autospec=True
        )
        raw_views = daily_metrics_extraction_factory("views")
        outputs = raw_views(build_asset_context(partition_key=DAY))
        return list(cast(Iterator[Output], outputs)), report.call_count

    return _run


def test_a_days_views_are_the_largest_listed_two_to_fifteen_days_later(run_raw_views):
    """Earlier archives hold partial counts, as does the last one to list a day."""
    (output,), _ = run_raw_views(
        {
            "2024-06-16": {DAY: 900},  # too soon to be trusted
            "2024-06-17": {DAY: 5, "2024-06-16": 70},
            "2024-06-20": {DAY: 8},
            "2024-06-29": {DAY: 2},  # clipped
            "2024-07-01": {DAY: 800},  # too late to list it
        }
    )

    core = cast(
        pd.DataFrame,
        core_github_views(build_asset_context(partition_key=DAY), output.value),
    )

    assert core["metrics_date"].astype(str).tolist() == [DAY]
    assert core["total_views"].tolist() == [8]


@pytest.mark.parametrize(
    ("archives", "now", "n_warnings"),
    [
        pytest.param({}, datetime(2024, 6, 17, 9, tzinfo=UTC), 0, id="not_yet_due"),
        pytest.param({}, datetime(2024, 6, 17, 11, tzinfo=UTC), 1, id="overdue"),
        pytest.param(
            {"2024-06-17": {"2024-06-14": 3}},
            datetime(2024, 6, 17, 11, tzinfo=UTC),
            0,
            id="no_traffic_that_day",
        ),
    ],
)
def test_a_day_without_views_only_warns_if_its_archives_are_missing(
    run_raw_views, archives, now, n_warnings
):
    outputs, warning_count = run_raw_views(archives, now)

    assert outputs == []
    assert warning_count == n_warnings
