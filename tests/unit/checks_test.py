"""Test how the pandera schema asset checks are wired into the Dagster jobs."""

import pandas as pd
import pyarrow as pa
import pytest
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetSelection,
    DailyPartitionsDefinition,
    Definitions,
    asset,
    build_asset_check_context,
    define_asset_job,
)

from usage_metrics.checks import EXAMPLES_PER_CHECK, _failure_report, _make_schema_check
from usage_metrics.defs import defs
from usage_metrics.models import _column, _table_schema, usage_metrics_schemas
from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager


def _upstream_nodes(job_name: str, node_name: str) -> set[str]:
    job = defs.resolve_job_def(job_name)
    upstream_outputs = (
        job.graph.dependency_structure.input_to_upstream_outputs_for_node(node_name)
    )
    return {output.node_name for outs in upstream_outputs.values() for output in outs}


def test_schema_check_blocks_assets_downstream_of_the_table() -> None:
    """A table whose data fails its schema must not feed anything downstream.

    For example, if AWS adds a field to the S3 logs and the columns are misaligned,
    the format checks fail, and out_s3_logs and the summaries built on it mustn't run.
    """
    assert "core_s3_logs_pandera_schema_check" in _upstream_nodes(
        "s3_metrics_etl", "out_s3_logs"
    )


TOY_SCHEMA = _table_schema(
    "toy",
    [_column("id", str), _column("tls", str, pattern=r"-|TLSv1\.[0-3]")],
    "A toy table.",
    primary_key=["id"],
)


def _run_check(table: pa.Table) -> AssetCheckResult:
    """Run the schema check for a toy table, handing it the table to check."""
    check = _make_schema_check("toy", TOY_SCHEMA, None)
    result = check(build_asset_check_context(), table=table)
    assert isinstance(result, AssetCheckResult)
    return result


def test_failing_check_reports_which_columns_failed_with_examples() -> None:
    table = pa.table(
        {
            "id": ["a", "a", None],  # a duplicate and a null key
            "tls": ["TLSv1.2x", "-garbage", "TLSv1.3"],  # two bad values
        }
    )

    result = _run_check(table)

    assert not result.passed
    assert result.severity == AssetCheckSeverity.ERROR
    assert result.description
    assert "toy" in result.description
    report = str(result.metadata["report"].value)
    assert "not_nullable" in report
    assert "multiple_fields_uniqueness" in report
    assert "TLSv1.2x" in report
    assert "-garbage" in report
    assert "failure_cases" in result.metadata


def test_failure_report_counts_failures_but_shows_only_a_few_examples() -> None:
    cases = pd.DataFrame(
        {
            "column": ["tls"] * 100,
            "check": ["str_matches"] * 100,
            "failure_case": [f"bad-{i}" for i in range(100)],
        }
    )

    report = _failure_report(cases)

    assert "100" in report
    assert f"bad-{EXAMPLES_PER_CHECK - 1}" in report
    assert f"bad-{EXAMPLES_PER_CHECK}" not in report


def _toy_job_result(tmp_path, monkeypatch, df: pd.DataFrame, partitioned: bool = True):
    """Materialize a toy asset and run its schema check in one job."""
    monkeypatch.setitem(usage_metrics_schemas, "toy", TOY_SCHEMA)
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
            _make_schema_check("toy", TOY_SCHEMA, partitions if partitioned else None)
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
    tmp_path, monkeypatch, partitioned
) -> None:
    """The check loads its table through the asset's IO manager, for its own partition."""
    df = pd.DataFrame({"id": ["a", "b"], "tls": ["-", "TLSv1.2"]})

    result = _toy_job_result(tmp_path, monkeypatch, df, partitioned)

    (evaluation,) = result.get_asset_check_evaluations()
    assert evaluation.passed
    assert evaluation.metadata["row_count"].value == 2


def test_check_reads_only_its_own_partition(tmp_path, monkeypatch) -> None:
    """Another day's file, which the asset's partitions also map to, isn't validated."""
    other_day = tmp_path / "toy" / "2026-01-01--2026-01-02.parquet"
    other_day.parent.mkdir()
    pd.DataFrame({"id": ["x", "x"], "tls": ["-garbage", "-"]}).to_parquet(other_day)
    df = pd.DataFrame({"id": ["a", "b"], "tls": ["-", "TLSv1.2"]})

    result = _toy_job_result(tmp_path, monkeypatch, df)

    (evaluation,) = result.get_asset_check_evaluations()
    assert evaluation.passed


def test_check_fails_the_job_when_the_written_table_breaks_its_schema(
    tmp_path, monkeypatch
) -> None:
    df = pd.DataFrame({"id": ["a", "a"], "tls": ["-garbage", "TLSv1.2"]})

    result = _toy_job_result(tmp_path, monkeypatch, df)

    assert not result.success
    (evaluation,) = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert "-garbage" in str(evaluation.metadata["report"].value)
