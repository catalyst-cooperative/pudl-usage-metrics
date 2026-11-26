"""Test what real assets do, run as Dagster jobs, for a day without input."""

from compression import zstd
from pathlib import Path

import pandas as pd
import pytest
from dagster import AssetSelection, Definitions, define_asset_job

from usage_metrics.core.kaggle import core_kaggle_logs
from usage_metrics.raw import eel_hole, github_partitioned, kaggle, s3, zenodo
from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager
from usage_metrics.scripts.etl import _execute

DAY = "2026-09-01"


class EmptyBucket:
    """A bucket with no raw files, and only an empty compacted S3 log."""

    def list_blobs(self, prefix=None):
        return []

    def get_blob(self, name):
        return self

    def download_to_filename(self, filename):
        Path(filename).write_bytes(zstd.compress(b""))


class EmptyClient:
    def bucket(self, name):
        return EmptyBucket()


@pytest.fixture(autouse=True)
def empty_gcs(monkeypatch, tmp_path):
    """Give every extractor a GCS client that finds no data."""
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", "gs://test-outputs")
    monkeypatch.setattr(
        "usage_metrics.raw.extract.GCSExtractor.gcs_client",
        property(lambda self: EmptyClient()),
    )
    monkeypatch.setattr(
        "usage_metrics.raw.extract.transfer_manager.download_many",
        lambda pairs, **kwargs: [],
    )


def _job(tmp_path, *assets):
    return Definitions(
        assets=list(assets),
        jobs=[define_asset_job("job", selection=AssetSelection.assets(*assets))],
        resources={
            "parquet_manager": PartitionedParquetIOManager(
                base_path=str(tmp_path / "parquet")
            )
        },
    ).resolve_job_def("job")


@pytest.mark.parametrize(
    "raw_asset",
    [
        eel_hole.raw_eel_hole_logs,
        *github_partitioned.raw_github_partitioned_assets,
        kaggle.raw_kaggle_logs,
        s3.raw_s3_logs,
        zenodo.raw_zenodo_logs,
    ],
    ids=lambda asset: asset.key.to_user_string(),
)
def test_a_day_without_input_succeeds_and_is_reported(tmp_path, raw_asset):
    """It mustn't fail forever, or pass unnoticed."""
    succeeded, missing_input = _execute(_job(tmp_path, raw_asset), partition_key=DAY)

    assert succeeded
    assert len(missing_input) == 1


def test_a_day_without_input_leaves_the_stored_table_alone(tmp_path):
    """Nothing downstream runs, so nothing is written over the day's output."""
    stored = tmp_path / "parquet" / "core_kaggle_logs" / f"{DAY}--2026-09-02.parquet"
    stored.parent.mkdir(parents=True)
    pd.DataFrame({"id": ["kept"]}).to_parquet(stored)
    before = stored.read_bytes()
    job = _job(tmp_path, kaggle.raw_kaggle_logs, core_kaggle_logs)

    succeeded, _ = _execute(job, partition_key=DAY)

    assert succeeded
    assert stored.read_bytes() == before
