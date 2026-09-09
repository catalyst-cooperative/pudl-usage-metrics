"""Tests for the partitioned GitHub metrics extractor."""

import json

from dagster import build_asset_context

from usage_metrics.raw.github_partitioned import GithubExtractor

BUCKET = "pudl-usage-metrics-archives.catalyst.coop"


def _ctx(partition_key: str):
    return build_asset_context(partition_key=partition_key)


def test_get_blob_prefix_is_metric_folder():
    """The prefix narrows the listing to one metric's folder."""
    assert GithubExtractor(metric="clones").get_blob_prefix(_ctx("2024-06-15")) == (
        "github/clones/"
    )


def test_filter_blobs_daily_metric_matches_exact_file(make_client):
    """A daily metric keeps only ``github/<metric>/<date>.json``."""
    blobs = {
        "github/clones/2024-06-15.json": b"{}",
        "github/clones/2024-06-16.json": b"{}",
        "github/views/2024-06-15.json": b"{}",
    }
    client = make_client({BUCKET: blobs})
    ext = GithubExtractor(metric="clones", client=client)
    listed = client.bucket(BUCKET).list_blobs()
    kept = [blob.name for blob in ext.filter_blobs(_ctx("2024-06-15"), listed)]
    assert kept == ["github/clones/2024-06-15.json"]


def test_filter_blobs_cumulative_metric_takes_newest(fake_blob):
    """A cumulative metric keeps only the most recently created file."""
    blobs = [
        fake_blob("github/stargazers/2024-01-01.json", b"[]", time_created=1),
        fake_blob("github/stargazers/2024-03-01.json", b"[]", time_created=3),
        fake_blob("github/stargazers/2024-02-01.json", b"[]", time_created=2),
    ]
    ext = GithubExtractor(metric="stargazers")
    kept = ext.filter_blobs(_ctx("2024-06-15"), blobs)
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
