#!/usr/bin/env python
"""This script pull github traffic metrics and saves them to a GC Bucket."""

import json
import os
import requests
import sys


from dataclasses import dataclass
from datetime import date
from google.cloud import storage
from requests.exceptions import HTTPError
from requests.models import Response


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
    
    response = make_github_request(query_url, headers)
    return json.dumps(response.json())


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
        metrics_json = make_github_request(query_url, headers, params).json()

        if len(metrics_json) <= 0:
            break
        metrics += metrics_json
        page += 1
    return json.dumps(metrics)

def make_github_request(query: str, headers: str, params: str=None):
    """
    Makes a request to the github api.

    Args:
        query (str): A github api request url.
        headers (str): Header to include in the request.
        params (str): Params of request.

    Returns:
        response (requests.models.Response): the request response. 
    """
    try:
        response = requests.get(query, headers=headers, params=params)

        response.raise_for_status()
    except HTTPError as http_err:
        raise HTTPError(f'HTTP error occurred: {http_err}\n\tResponse test: {response.text}')
    except Exception as err:
        raise Exception(f'Other error occurred: {err}') 
    return response

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
