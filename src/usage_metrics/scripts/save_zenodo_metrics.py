"""This script pull github traffic metrics and saves them to a GC Bucket."""

import logging
import sys
from datetime import date, datetime
from typing import Annotated

import pandas as pd
import requests
from google.cloud import storage
from pydantic import BaseModel, StringConstraints

Doi = Annotated[str, StringConstraints(pattern=r"10\.5281/zenodo\.\d+")]
SandboxDoi = Annotated[str, StringConstraints(pattern=r"10\.5072/zenodo\.\d+")]

logger = logging.getLogger()
logging.basicConfig(level="INFO")


class ZenodoStats(BaseModel):
    """Pydantic model representing Zenodo usage stats.

    See https://developers.zenodo.org/#representation.
    """

    downloads: int
    unique_downloads: int
    views: int
    unique_views: int
    version_downloads: int
    version_unique_downloads: int
    version_unique_views: int
    version_views: int


class CommunityMetadata(BaseModel):
    """Pydantic model representing Zenodo deposition metadata from the communities endpoint.

    See https://developers.zenodo.org/#representation.
    """

    created: datetime = None
    modified: datetime = None
    recid: str
    conceptrecid: str
    doi: Doi | SandboxDoi | None = None
    conceptdoi: Doi | SandboxDoi | None = None
    doi_url: str
    title: str
    updated: datetime.datetime = None
    stats: ZenodoStats

    @classmethod
    def check_empty_string(cls, doi: str):  # noqa: N805
        """Sometimes zenodo returns an empty string for the `doi`. Convert to None."""
        if doi == "":
            return


def get_zenodo_logs() -> pd.DataFrame():
    """Get all metrics for all versions of records in the Catalyst Cooperative Zenodo community."""
    community_url = "https://zenodo.org/api/communities/14454015-63f1-4f05-80fd-1a9b07593c9e/records"
    community_records = requests.get(community_url, timeout=100)
    catalyst_records = community_records.json()["hits"]["hits"]
    catalyst_records = [CommunityMetadata(**record) for record in catalyst_records]
    stats_dfs = []

    for record in catalyst_records:
        logger.info(f"Getting usage metrics for {record.title}")
        # For each record in the community
        versions_url = f"https://zenodo.org/api/records/{record.recid}/versions"
        record_versions = requests.get(versions_url, timeout=100)
        version_records = record_versions.json()["hits"]["hits"]
        version_records = [CommunityMetadata(**record) for record in version_records]
        version_df = pd.DataFrame(
            [
                dict(
                    version_records[item].stats.__dict__,
                    doi=version_records[item].doi,
                    title=version_records[item].title,
                )
                for item in range(len(version_records))
            ]
        )
        if not version_df.empty:
            stats_dfs.append(version_df)
    return pd.concat(stats_dfs)


def upload_to_bucket(data: pd.DataFrame) -> None:
    """Upload a gcp object."""
    bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    bucket.blob(f"zenodo/{date.today().strftime('%Y-%m-%d')}.csv").upload_from_string(
        data.to_csv(index=False), "text/csv"
    )

    logger.info("Uploaded data to GCS bucket.")


def save_metrics():
    """Save Zenodo traffic metrics to google cloud bucket."""
    metrics_df = get_zenodo_logs()
    upload_to_bucket(metrics_df)


if __name__ == "__main__":
    sys.exit(save_metrics())


# class ZenodoClientError(Exception):
#     """Captures the JSON error information from Zenodo."""

#     def __init__(self, status, message, errors=None):
#         """Constructor.

#         Args:
#             status: status message of response
#             message: message of response
#             errors: if any, list of errors returned by response
#         """
#         self.status = status
#         self.message = message
#         self.errors = errors

#     def __str__(self):
#         """Cast to string."""
#         return repr(self)

#     def __repr__(self):
#         """But the kwargs are useful for recreating this object."""
#         return f"ZenodoClientError(status={self.status}, message={self.message}, errors={self.errors})"


# class DepositionCreator(BaseModel):
#     """Pydantic model representing zenodo deposition creators.

#     See https://developers.zenodo.org/#representation.
#     """

#     name: str
#     affiliation: str | None = None

#     @classmethod
#     def from_contributor(cls, contributor: Contributor) -> "DepositionCreator":
#         """Construct deposition metadata object from PUDL Contributor model."""
#         return cls(name=contributor.title, affiliation=contributor.organization)


# class DepositionMetadata(BaseModel):
#     """Pydantic model representing zenodo deposition metadata.

#     See https://developers.zenodo.org/#representation.
#     """

#     upload_type: str = "dataset"
#     publication_date: datetime.date = None
#     language: str = "eng"
#     title: str
#     creators: list[DepositionCreator]
#     description: str
#     access_right: str = "open"
#     license_: str = Field(alias="license")
#     doi: Doi | SandboxDoi | None = None
#     prereserve_doi: dict | bool = False
#     keywords: list[str] | None = None
#     version: str | None = None

#     @field_validator("doi", mode="before")
#     @classmethod
#     def check_empty_string(cls, doi: str):  # noqa: N805
#         """Sometimes zenodo returns an empty string for the `doi`. Convert to None."""
#         if doi == "":
#             return


# class BucketFile(BaseModel):
#     """Pydantic model representing zenodo file metadata returned by bucket api.

#     See https://developers.zenodo.org/#quickstart-upload.
#     """

#     key: str
#     mimetype: str
#     checksum: str
#     version_id: str
#     size: int
#     created: datetime.datetime
#     updated: datetime.datetime
#     links: FileLinks
#     is_head: bool
#     delete_marker: bool


# class Deposition(BaseModel):
#     """Pydantic model representing a zenodo deposition.

#     See https://developers.zenodo.org/#depositions.
#     """

#     conceptdoi: Doi | SandboxDoi | None = None
#     conceptrecid: str
#     created: datetime.datetime
#     files: list[DepositionFile] = []
#     id_: int = Field(alias="id")
#     metadata: DepositionMetadata
#     modified: datetime.datetime
#     links: DepositionLinks
#     owner: int
#     record_id: int
#     record_url: Url | None = None
#     state: Literal["inprogress", "done", "error", "submitted", "unsubmitted"]
#     submitted: bool
#     title: str

#     @property
#     def files_map(self) -> dict[str, DepositionFile]:
#         """Files associated with their filenames."""
#         return {f.filename: f for f in self.files}


# class Record(BaseModel):
#     """The /records/ endpoints return a slightly different data structure."""

#     id_: int = Field(alias="id")
#     links: DepositionLinks


# @dataclass
# class Metric:
#     """Format metrics into folder names."""

#     name: str
#     folder: str


# def get_biweekly_metrics(owner: str, repo: str, token: str, metric: str) -> str:
#     """Get json data for a biweekly github metric.

#     Args:
#         metric (str): The github metric name.

#     Returns:
#         json (str): The metric data as json text.
#     """
#     query_url = f"https://api.github.com/repos/{owner}/{repo}/traffic/{metric}"
#     headers = {
#         "Authorization": f"token {token}",
#         "Accept": "application/vnd.github.v3+json",
#     }

#     response = make_github_request(query_url, headers)
#     return json.dumps(response.json())


# def get_persistent_metrics(owner: str, repo: str, token: str, metric: str) -> str:
#     """Get githubs persistent metrics: forks and stargazers.

#     Args:
#         metrics (str): the metric to retrieve (forks | stargazers)

#     Returns:
#         json (str): A json string of metrics.
#     """
#     query_url = f"https://api.github.com/repos/{owner}/{repo}/{metric}"
#     headers = {
#         "Authorization": f"token {token}",
#         "Accept": "application/vnd.github.v3.star+json",
#     }

#     metrics = []
#     page = 1

#     timeout = 600  # Set 10 minute timeout
#     timeout_start = time.time()

#     while time.time() < timeout_start + timeout:
#         params = {"page": page}
#         metrics_json = make_github_request(query_url, headers, params).json()

#         if len(metrics_json) <= 0:
#             break
#         metrics += metrics_json
#         page += 1
#     return json.dumps(metrics)


# def make_github_request(query: str, headers: str, params: str = None):
#     """Makes a request to the github api.

#     Args:
#         query (str): A github api request url.
#         headers (str): Header to include in the request.
#         params (str): Params of request.

#     Returns:
#         response (requests.models.Response): the request response.
#     """
#     try:
#         response = requests.get(query, headers=headers, params=params, timeout=100)

#         response.raise_for_status()
#     except HTTPError as http_err:
#         raise HTTPError(
#             f"HTTP error occurred: {http_err}\n\tResponse test: {response.text}"
#         )
#     return response


# def upload_to_bucket(data, metric):
#     """Upload a gcp object."""
#     bucket_name = "pudl-usage-metrics-archives.catalyst.coop"
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob_name = f"github/{metric.folder}/{date.today().strftime('%Y-%m-%d')}.json"

#     blob = bucket.blob(blob_name)
#     blob.upload_from_string(data)

#     logger.info(f"Uploaded {metric.name} data to {blob_name}.")


# def save_metrics():
#     """Save github traffic metrics to google cloud bucket."""
#     token = os.getenv("API_TOKEN_GITHUB", "...")
#     owner = "catalyst-cooperative"
#     repo = "pudl"

#     biweekly_metrics = [
#         Metric("clones", "clones"),
#         Metric("popular/paths", "popular_paths"),
#         Metric("popular/referrers", "popular_referrers"),
#         Metric("views", "views"),
#     ]
#     persistent_metrics = [Metric("stargazers", "stargazers"), Metric("forks", "forks")]

#     for metric in biweekly_metrics:
#         metric_data = get_biweekly_metrics(owner, repo, token, metric.name)
#         upload_to_bucket(metric_data, metric)

#     for metric in persistent_metrics:
#         metric_data = get_persistent_metrics(owner, repo, token, metric.name)
#         upload_to_bucket(metric_data, metric)


# if __name__ == "__main__":
#     sys.exit(save_metrics())
