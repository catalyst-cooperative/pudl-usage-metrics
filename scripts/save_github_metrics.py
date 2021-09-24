#!/usr/bin/env python
"""This script pull github traffic metrics and saves them to a GC Bucket."""

import json
import os
from datetime import date
import sys

import requests
from google.cloud import storage

from dataclasses import dataclass

@dataclass
class Metric:
    name: str
    folder: str


TOKEN = os.getenv("API_TOKEN_GITHUB", "...")
OWNER = "catalyst-cooperative"
REPO = "pudl"
BUCKET_NAME = "github-metrics"

BIWEEKLY_METRICS = [Metric("clones", "clones"), Metric("popular/paths", "popular_paths"), Metric("popular/referrers", "popular_referrers"), Metric("views", "views")]
PERSISTENT_METRICS = [Metric("stargazers", "stargazers"), Metric("forks", "forks")]


def get_biweekly_metrics(metric: str) -> str:
    """
    Get json data for a biweekly github metric.

    Args:
        metric (str): The github metric name.
    Returns:
        json (str): The metric data as json text.
    """
    query_url = f"https://api.github.com/repos/{OWNER}/{REPO}/traffic/{metric}"
    headers = {
        "Authorization": f"token {TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    r = requests.get(query_url, headers=headers)
    return json.dumps(r.json())


def get_persistent_metrics(metric) -> str:
    """
    Get githubs persistent metrics: forks and stargazers.

    Args:
        metrics (str): the metric to retrieve (forks | stargazers)
    Returns:
        json (str): A json string of metrics.
    """
    query_url = f"https://api.github.com/repos/{OWNER}/{REPO}/{metric}"
    headers = {
        "Authorization": f"token {TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    metrics = []
    page = 1
    while True:
        params = {"page": page}
        metrics_json = requests.get(query_url, headers=headers, params=params).json()
        if len(metrics_json) <= 0:
            break
        metrics += metrics_json
        page += 1
    return json.dumps(metrics)


def upload_to_bucket(data, metric):
    """Upload a gcp object."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"{metric.folder}/{date.today().strftime('%Y-%m-%d')}.json"
    
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

    print(f"Uploaded {metric.name} data to {blob_name}.")

def save_metrics():
    """Save github traffic metrics to google cloud bucket."""
    for metric in BIWEEKLY_METRICS:
        metric_data = get_biweekly_metrics(metric.name)
        upload_to_bucket(metric_data, metric)

    for metric in PERSISTENT_METRICS:
        metric_data = get_persistent_metrics(metric.name)
        upload_to_bucket(metric_data, metric)

if __name__ == "__main__":
    sys.exit(save_metrics())
