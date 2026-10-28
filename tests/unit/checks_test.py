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
from usage_metrics.models import _column, _table_schema, usage_metrics_schemas
from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager

TOY_SCHEMA = _table_schema(
    "toy",
    [
        _column("id", str, "ID."),
        _column("tls", str, "TLS version.", pattern=r"-|TLSv1\.[0-3]"),
    ],
    "A toy table.",
    primary_key=["id"],
)


def _run_check(table: pa.Table) -> AssetCheckResult:
    """Run the schema check for a toy table, handing it the table to check."""
    check = _make_schema_check("toy", TOY_SCHEMA, None)
    result = check(build_asset_check_context(), table=table)
    assert isinstance(result, AssetCheckResult)
    return result


def _report_rows(result: AssetCheckResult) -> dict[tuple[str, str], tuple[int, str]]:
    """Read the failure report into {(column, check name): (failure count, examples)}."""
    rows = {}
    _header, *lines = str(result.metadata["report"].value).splitlines()
    for line in lines:
        column, check, failures, examples = line.split(maxsplit=3)
        rows[column, check.split("(")[0]] = (int(failures), examples)
    return rows


def test_failing_check_reports_which_columns_failed_with_examples() -> None:
    """A failed check says which columns broke which checks, how often, and for what.

    The report has one row for each column and check that failed, with the number of
    failures and a few of the failing values. The null key fails the `id` column's
    not-null check, while the duplicate fails the uniqueness check of the primary key,
    which belongs to the table rather than to any one column.
    """
    table = pa.table(
        {
            "id": ["a", "a", None],  # a duplicate and a null key
            "tls": ["TLSv1.2x", "-garbage", "TLSv1.3"],  # two bad values
        }
    )

    result = _run_check(table)

    assert not result.passed
    assert result.severity == AssetCheckSeverity.ERROR
    rows = _report_rows(result)
    assert {key: count for key, (count, _) in rows.items()} == {
        ("id", "not_nullable"): 1,
        ("tls", "str_matches"): 2,
        ("toy", "multiple_fields_uniqueness"): 1,
    }
    tls_examples = rows["tls", "str_matches"][1]
    assert "TLSv1.2x" in tls_examples
    assert "-garbage" in tls_examples


def test_failure_report_counts_failures_but_shows_only_a_few_examples() -> None:
    """A column and check with many failures is one line: a count and a few examples.

    A real table can have many thousands of failing values, which would otherwise flood the log and the Dagster UI.
    """
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
    """A table that breaks its schema fails its blocking check, and so the job."""
    df = pd.DataFrame({"id": ["a", "a"], "tls": ["-garbage", "TLSv1.2"]})

    result = _toy_job_result(tmp_path, monkeypatch, df)

    assert not result.success
    (evaluation,) = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert "-garbage" in str(evaluation.metadata["report"].value)
