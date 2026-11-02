"""Tests for the partitioned GitHub metrics extractor."""

import json
from typing import cast

from google.cloud import storage

from usage_metrics.raw.github_partitioned import GithubExtractor

from .conftest import FakeBlob


def test_filter_blobs_daily_metric_matches_exact_file(filtered_blob_names):
    """A daily metric keeps only ``github/<metric>/<date>.json``."""
    kept = filtered_blob_names(
        GithubExtractor,
        "2024-06-15",
        [
            "github/clones/2024-06-15.json",
            "github/clones/2024-06-16.json",
            "github/views/2024-06-15.json",
        ],
        metric="clones",
    )
    assert kept == ["github/clones/2024-06-15.json"]


def test_filter_blobs_cumulative_metric_takes_newest(partition_context):
    """A cumulative metric keeps only the most recently created file."""
    blobs = [
        FakeBlob("github/stargazers/2024-01-01.json", b"[]", time_created=1),
        FakeBlob("github/stargazers/2024-03-01.json", b"[]", time_created=3),
        FakeBlob("github/stargazers/2024-02-01.json", b"[]", time_created=2),
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
