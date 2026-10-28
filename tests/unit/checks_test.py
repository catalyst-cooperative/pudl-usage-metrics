"""Test the pandera schema asset check on its own: its results and failure reports."""

import pandas as pd
import pandera.pyarrow as pandera
import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    build_asset_check_context,
)

from usage_metrics.checks import EXAMPLES_PER_CHECK, _failure_report, _make_schema_check


def _run_check(schema: pandera.DataFrameSchema, table: pa.Table) -> AssetCheckResult:
    """Run the schema check for a toy table, handing it the table to check."""
    check = _make_schema_check("toy", schema, None)
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


def test_failing_check_reports_which_columns_failed_with_examples(toy_schema) -> None:
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

    result = _run_check(toy_schema, table)

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
