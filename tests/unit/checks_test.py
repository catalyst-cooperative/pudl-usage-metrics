"""Test how the pandera schema asset checks are wired into the Dagster jobs."""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetCheckResult, AssetCheckSeverity, build_asset_check_context

from usage_metrics.checks import EXAMPLES_PER_CHECK, _failure_report, _make_schema_check
from usage_metrics.etl import defs
from usage_metrics.models import _column, _table_schema
from usage_metrics.resources.parquet_io_manager import PyArrowTableReader


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


def _run_check(tmp_path, table: pa.Table) -> AssetCheckResult:
    """Run the schema check for a toy table that is stored in tmp_path."""
    schema = _table_schema(
        "toy",
        [_column("id", str), _column("tls", str, pattern=r"-|TLSv1\.[0-3]")],
        "A toy table.",
        primary_key=["id"],
    )
    pq.write_table(table, tmp_path / "toy.parquet")
    context = build_asset_check_context(
        resources={"pyarrow_reader": PyArrowTableReader(base_path=str(tmp_path))}
    )
    result = _make_schema_check("toy", schema)(context)
    assert isinstance(result, AssetCheckResult)
    return result


def test_passing_check_reports_the_row_count(tmp_path) -> None:
    result = _run_check(tmp_path, pa.table({"id": ["a", "b"], "tls": ["-", None]}))

    assert result.passed
    assert result.metadata["row_count"].value == 2


def test_failing_check_reports_which_columns_failed_with_examples(tmp_path) -> None:
    table = pa.table(
        {
            "id": ["a", "a", None],  # a duplicate and a null key
            "tls": ["TLSv1.2x", "-garbage", "TLSv1.3"],  # two bad values
        }
    )

    result = _run_check(tmp_path, table)

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
