"""Scenario tests: no run may lose data that was stored by an earlier one.

Each scenario runs a source's real assets and asset checks as a Dagster job, with the
real Parquet IO manager writing under a temporary directory, and GCS faked in memory.
They describe the guarantees that a backfill relies on, rather than how any one asset
provides them.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from dagster import AssetSelection, Definitions, define_asset_job

from usage_metrics import helpers
from usage_metrics.checks import build_schema_checks
from usage_metrics.defs import default_asset_checks, default_assets
from usage_metrics.paths import (
    PUDL_METRICS_ARCHIVES_BUCKET,
    PUDL_METRICS_EEL_HOLE_LOGS_BUCKET,
    PUDL_METRICS_S3_LOGS_BUCKET,
)
from usage_metrics.raw.extract import GCSExtractor
from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager
from usage_metrics.scripts.etl import _execute

DAY = "2026-09-01"
NEXT_DAY = "2026-09-02"
THIRD_DAY = "2026-09-03"
OUTPUT_BUCKET = "test-outputs"


def _s3_record(day: str, request_id: str) -> str:
    made = pd.Timestamp(day).strftime("%d/%b/%Y")
    return (
        f"{'a' * 64} pudl.catalyst.coop [{made}:12:00:00 +0000] 192.0.2.3 - "
        f"{request_id} REST.GET.OBJECT stable/core_eia__codes.parquet "
        '"GET /k HTTP/1.1" 200 - 100 200 5 4 "-" "curl" - hid SigV4 ECDHE AuthHeader '
        "host TLSv1.2 - - us-east-1\n"
    )


def _s3_logs(day: str) -> dict[tuple[str, str], bytes]:
    return {
        (PUDL_METRICS_S3_LOGS_BUCKET, f"{day}-12-00-00-{i}"): _s3_record(
            day, f"{day}-{i}"
        ).encode()
        for i in range(3)
    }


def _eel_hole_logs(event: str = "search") -> dict[tuple[str, str], bytes]:
    record = {
        "insertId": "a",
        "jsonPayload": {
            "event": event,
            "timestamp": f"{DAY}T00:00:00Z",
            "url": "/search?q=coal",
            "query": "coal",
            "user_id": None,
            "user_domain": None,
        },
        "labels": {"instanceId": "0e1b2c3d"},
        "logName": "projects/x/logs/run.googleapis.com%2Fstdout",
        "receiveTimestamp": f"{DAY}T00:00:00Z",
        "resource": {
            "type": "cloud_run_revision",
            "labels": {
                "configuration_name": "pudl-viewer",
                "location": "us-east1",
                "project_id": "catalyst-cooperative-pudl",
                "revision_name": "pudl-viewer-00001-abc",
                "service_name": "pudl-viewer",
            },
        },
        "timestamp": f"{DAY}T00:00:00Z",
        "textPayload": None,
    }
    name = f"run.googleapis.com/stdout/{DAY.replace('-', '/')}/00:00:00_S0.json"
    return {(PUDL_METRICS_EEL_HOLE_LOGS_BUCKET, name): json.dumps(record).encode()}


def _github_archives() -> dict[tuple[str, str], bytes]:
    counts = [{"timestamp": f"{DAY}T00:00:00Z", "count": 5, "uniques": 2}]
    contents = {
        "views/2026-09-03": {"views": counts},
        "clones/2026-09-03": {"clones": counts},
        f"popular_paths/{DAY}": [{"path": "/pudl", "count": 9, "uniques": 3}],
        f"popular_referrers/{DAY}": [{"referrer": "x.com", "count": 9, "uniques": 3}],
    }
    return {
        (PUDL_METRICS_ARCHIVES_BUCKET, f"github/{name}.json"): json.dumps(data).encode()
        for name, data in contents.items()
    }


def _kaggle_archive() -> dict[tuple[str, str], bytes]:
    data = {"datasetId": 1, "datasetSlug": "pudl-project", "totalViews": 7}
    data["metrics_date"] = DAY
    return {
        (PUDL_METRICS_ARCHIVES_BUCKET, f"kaggle/{DAY}.json"): json.dumps(data).encode()
    }


def _zenodo_archive() -> dict[tuple[str, str], bytes]:
    counts = [
        "downloads",
        "unique_downloads",
        "views",
        "unique_views",
        "version_downloads",
        "version_unique_downloads",
        "version_views",
        "version_unique_views",
    ]
    record = {
        "id": 1,
        "recid": 1,
        "conceptrecid": "7067366",
        "doi": "10.5281/zenodo.1",
        "conceptdoi": "10.5281/zenodo.7067366",
        "doi_url": "https://doi.org/10.5281/zenodo.1",
        "title": "A dataset",
        "status": "published",
        "state": "done",
        "submitted": True,
        "created": f"{DAY}T00:00:00+00:00",
        "modified": f"{DAY}T00:00:00+00:00",
        "updated": f"{DAY}T00:00:00+00:00",
        "metadata": {"publication_date": DAY, "description": "x", "version": "v1"},
        "stats": dict.fromkeys(counts, 1),
    }
    return {
        (PUDL_METRICS_ARCHIVES_BUCKET, f"zenodo/{DAY}-1.json"): json.dumps(
            {"hits": {"hits": [record]}}
        ).encode()
    }


SOURCES = {
    "s3": lambda: _s3_logs(DAY) | _s3_logs(NEXT_DAY),
    "eel_hole": _eel_hole_logs,
    "github_partitioned": _github_archives,
    "kaggle": _kaggle_archive,
    "zenodo": _zenodo_archive,
}
"""Raw input that gives each source's job something to store for ``DAY``."""


@dataclass
class Pipeline:
    """The real assets, running against fake GCS and a temporary output directory."""

    gcs: Any  # The fake client from the fake_gcs fixture
    outputs: Path

    def add(self, blobs: dict[tuple[str, str], bytes]) -> None:
        for (bucket, name), data in blobs.items():
            self.gcs.bucket(bucket).blob(name).upload_from_string(data)

    def remove(self, blobs: dict[tuple[str, str], bytes]) -> None:
        for bucket, name in blobs:
            del self.gcs.bucket(bucket)._blobs[name]

    def run(self, source: str, day: str = DAY, **kwargs) -> tuple[bool, list[str]]:
        job = Definitions(
            assets=default_assets,
            asset_checks=default_asset_checks + build_schema_checks(default_assets),
            jobs=[define_asset_job("job", AssetSelection.tag("source", source))],
            resources={
                "parquet_manager": PartitionedParquetIOManager(
                    base_path=str(self.outputs)
                )
            },
        ).resolve_job_def("job")
        return _execute(job, partition_key=day, **kwargs)

    def tables(self) -> dict[str, pd.DataFrame]:
        """Every stored Parquet file, by its path."""
        return {
            f"{path.parent.name}/{path.name}": pd.read_parquet(path)
            for path in sorted(self.outputs.rglob("*.parquet"))
        }

    def artifacts(self) -> dict[str, bytes]:
        """Every compacted S3 log."""
        return {
            blob.name: blob.download_as_bytes()
            for blob in self.gcs.bucket(OUTPUT_BUCKET).list_blobs()
        }


@pytest.fixture
def pipeline(monkeypatch, tmp_path, fake_gcs, patch_download_many) -> Pipeline:
    buckets = [
        PUDL_METRICS_ARCHIVES_BUCKET,
        PUDL_METRICS_EEL_HOLE_LOGS_BUCKET,
        PUDL_METRICS_S3_LOGS_BUCKET,
        OUTPUT_BUCKET,
    ]
    gcs = fake_gcs({bucket: {} for bucket in buckets})
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", f"gs://{OUTPUT_BUCKET}")
    monkeypatch.delenv("METRICS_PROD_ENV", raising=False)
    monkeypatch.setattr(GCSExtractor, "gcs_client", property(lambda self: gcs))
    monkeypatch.setattr(helpers, "geocode_ip", lambda ip: {"ip": ip})
    return Pipeline(gcs, tmp_path / "parquet")


def _assert_unchanged(pipeline: Pipeline, before: dict[str, pd.DataFrame]) -> None:
    after = pipeline.tables()
    assert after.keys() == before.keys()
    for name, table in before.items():
        pd.testing.assert_frame_equal(after[name], table)


@pytest.mark.parametrize("source", SOURCES)
def test_a_day_without_input_can_be_filled_later(pipeline, source):
    succeeded, warnings = pipeline.run(source)
    assert succeeded
    assert warnings
    assert pipeline.tables() == {}
    assert pipeline.artifacts() == {}

    pipeline.add(SOURCES[source]())
    succeeded, _ = pipeline.run(source)

    assert succeeded
    assert any(len(table) for table in pipeline.tables().values())


@pytest.mark.parametrize("source", SOURCES)
def test_losing_the_raw_input_changes_nothing_stored(pipeline, source):
    """Expired or deleted raw input, then the same day run again."""
    raw = SOURCES[source]()
    pipeline.add(raw)
    assert pipeline.run(source)[0]
    tables, artifacts = pipeline.tables(), pipeline.artifacts()
    assert any(len(table) for table in tables.values())

    pipeline.remove(raw)
    succeeded, _ = pipeline.run(source)

    assert succeeded
    _assert_unchanged(pipeline, tables)
    assert pipeline.artifacts() == artifacts


def test_results_that_turn_out_empty_fail_and_keep_the_data(pipeline):
    """As a parsing bug would: there is input, but nothing comes out of it."""
    pipeline.add(_eel_hole_logs())
    assert pipeline.run("eel_hole")[0]
    tables = pipeline.tables()

    pipeline.add(_eel_hole_logs("loading"))
    succeeded, _ = pipeline.run("eel_hole")

    assert not succeeded
    for name, table in tables.items():
        if len(table):
            pd.testing.assert_frame_equal(pipeline.tables()[name], table)


def test_consecutive_s3_days_count_each_request_once(pipeline):
    """Including a request logged the day after it was made. One logged much later
    is kept, but left out of the summaries, which already cover the day it was made.
    """
    late = {
        (PUDL_METRICS_S3_LOGS_BUCKET, f"{NEXT_DAY}-00-05-00-late"): _s3_record(
            DAY, "late"
        ).encode(),
        (PUDL_METRICS_S3_LOGS_BUCKET, f"{DAY}-00-05-00-straggler"): _s3_record(
            "2026-08-20", "straggler"
        ).encode(),
    }
    pipeline.add(_s3_logs(DAY) | _s3_logs(NEXT_DAY) | _s3_logs(THIRD_DAY) | late)

    assert pipeline.run("s3", DAY)[0]
    assert pipeline.run("s3", NEXT_DAY)[0]

    by_table: dict[str, list[pd.DataFrame]] = {}
    for path, table in pipeline.tables().items():
        by_table.setdefault(path.split("/")[0], []).append(table)
    stored = {name: pd.concat(frames) for name, frames in by_table.items()}
    assert len(stored["core_s3_logs"]) == 8
    for name, table in stored.items():
        assert table["id"].is_unique, name
        if "request_count" in table:
            assert table["request_count"].sum() == 7, name
