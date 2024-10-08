"""This script pull github traffic metrics and saves them to a GC Bucket."""

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date

import requests
from google.cloud import storage

logger = logging.getLogger()
logging.basicConfig(level="INFO")


@dataclass
class Metric:
    """Format metrics into folder names."""

    name: str
    folder: str


def get_biweekly_metrics(owner: str, repo: str, token: str, metric: str) -> str:
    """Get json data for a biweekly github metric.

    Args:
        metric (str): The github metric name.

    Returns:
        json (str): The metric data as json text.
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/traffic/{metric}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.get(url, headers=headers, timeout=100)
    return json.dumps(response.json())


def get_persistent_metrics(owner: str, repo: str, token: str, metric: str) -> str:
    """Get githubs persistent metrics: forks and stargazers.

    Args:
        metrics (str): the metric to retrieve (forks | stargazers)

    Returns:
        json (str): A json string of metrics.
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/{metric}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3.star+json",
    }

    metrics = []
    page = 1

    timeout = 600  # Set 10 minute timeout
    timeout_start = time.time()

    while time.time() < timeout_start + timeout:
        params = {"page": page}
        metrics_json = requests.get(
            url=url, headers=headers, params=params, timeout=100
        ).json()

        if len(metrics_json) <= 0:
            break
        metrics += metrics_json
        page += 1
    return json.dumps(metrics)


def upload_to_bucket(data, metric):
    """Upload a gcp object."""
    bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = f"github/{metric.folder}/{date.today().strftime('%Y-%m-%d')}.json"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

    logger.info(f"Uploaded {metric.name} data to {blob_name}.")


def save_metrics():
    """Save github traffic metrics to google cloud bucket."""
    token = os.getenv("API_TOKEN_GITHUB", "...")
    owner = "catalyst-cooperative"
    repo = "pudl"

    biweekly_metrics = [
        Metric("clones", "clones"),
        Metric("popular/paths", "popular_paths"),
        Metric("popular/referrers", "popular_referrers"),
        Metric("views", "views"),
    ]
    persistent_metrics = [Metric("stargazers", "stargazers"), Metric("forks", "forks")]

    for metric in biweekly_metrics:
        metric_data = get_biweekly_metrics(owner, repo, token, metric.name)
        upload_to_bucket(metric_data, metric)

    for metric in persistent_metrics:
        metric_data = get_persistent_metrics(owner, repo, token, metric.name)
        upload_to_bucket(metric_data, metric)


if __name__ == "__main__":
    sys.exit(save_metrics())
