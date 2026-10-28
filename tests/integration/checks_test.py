"""Test the pandera schema asset check in real Dagster jobs, with the Parquet IO manager."""

import pandas as pd
import pytest
from dagster import (
    AssetSelection,
    DailyPartitionsDefinition,
    Definitions,
    asset,
    define_asset_job,
)

from usage_metrics.checks import _make_schema_check
from usage_metrics.models import usage_metrics_schemas
from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager


def _toy_job_result(
    schema, tmp_path, monkeypatch, df: pd.DataFrame, partitioned: bool = True
):
    """Materialize a toy asset and run its schema check in one job."""
    monkeypatch.setitem(usage_metrics_schemas, "toy", schema)
    partitions = DailyPartitionsDefinition(start_date="2026-01-01")

    @asset(
        name="toy",
        partitions_def=partitions if partitioned else None,
        io_manager_key="parquet_manager",
    )
    def toy() -> pd.DataFrame:
        return df

    job = define_asset_job(
        "toy_job",
        selection=AssetSelection.assets(toy) | AssetSelection.checks_for_assets(toy),
    )
    toy_defs = Definitions(
        assets=[toy],
        asset_checks=[
            _make_schema_check("toy", schema, partitions if partitioned else None)
        ],
        jobs=[job],
        resources={
            "parquet_manager": PartitionedParquetIOManager(base_path=str(tmp_path))
        },
    )
    return toy_defs.resolve_job_def("toy_job").execute_in_process(
        partition_key="2026-01-02" if partitioned else None, raise_on_error=False
    )


@pytest.mark.parametrize("partitioned", [True, False])
def test_check_validates_the_table_the_io_manager_wrote(
    toy_schema, tmp_path, monkeypatch, partitioned
) -> None:
    """The check loads its table through the asset's IO manager, for its own partition."""
    df = pd.DataFrame({"id": ["a", "b"], "tls": ["-", "TLSv1.2"]})

    result = _toy_job_result(toy_schema, tmp_path, monkeypatch, df, partitioned)

    (evaluation,) = result.get_asset_check_evaluations()
    assert evaluation.passed
    assert evaluation.metadata["row_count"].value == 2


def test_check_reads_only_its_own_partition(toy_schema, tmp_path, monkeypatch) -> None:
    """Another day's file, which the asset's partitions also map to, isn't validated."""
    other_day = tmp_path / "toy" / "2026-01-01--2026-01-02.parquet"
    other_day.parent.mkdir()
    pd.DataFrame({"id": ["x", "x"], "tls": ["-garbage", "-"]}).to_parquet(other_day)
    df = pd.DataFrame({"id": ["a", "b"], "tls": ["-", "TLSv1.2"]})

    result = _toy_job_result(toy_schema, tmp_path, monkeypatch, df)

    (evaluation,) = result.get_asset_check_evaluations()
    assert evaluation.passed


def test_check_fails_the_job_when_the_written_table_breaks_its_schema(
    toy_schema, tmp_path, monkeypatch
) -> None:
    """A table that breaks its schema fails its blocking check, and so the job."""
    df = pd.DataFrame({"id": ["a", "a"], "tls": ["-garbage", "TLSv1.2"]})

    result = _toy_job_result(toy_schema, tmp_path, monkeypatch, df)

    assert not result.success
    (evaluation,) = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert "-garbage" in str(evaluation.metadata["report"].value)
