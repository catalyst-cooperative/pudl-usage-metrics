"""Tests for `usage_metrics.scripts.etl`."""

import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from click.testing import CliRunner
from dagster import (
    AssetCheckResult,
    Definitions,
    asset,
    asset_check,
    define_asset_job,
)

from usage_metrics.scripts import etl as etl_module
from usage_metrics.scripts.etl import _execute, etl


def _fake_job_def(name: str, partition_keys: list[str] | None):
    partitions_def = None
    if partition_keys is not None:
        partitions_def = SimpleNamespace(get_partition_keys=lambda: partition_keys)
    return SimpleNamespace(name=name, partitions_def=partitions_def)


_ALL_PARTITIONED = _fake_job_def(
    "all_partitioned_metrics_etl", ["2026-09-24", "2026-09-25"]
)
_ALL_NONPARTITIONED = _fake_job_def("all_nonpartitioned_metrics_etl", None)
_EEL_HOLE = _fake_job_def("eel_hole_metrics_etl", ["2026-09-24", "2026-09-25"])
_GITHUB_NONPARTITIONED = _fake_job_def("github_nonpartitioned_metrics_etl", None)

_JOB_DEFS_BY_NAME = {
    j.name: j
    for j in [_ALL_PARTITIONED, _ALL_NONPARTITIONED, _EEL_HOLE, _GITHUB_NONPARTITIONED]
}


@pytest.fixture
def executed(monkeypatch):
    """Stub out defs.resolve_job_def and _execute; record every _execute call."""
    calls: list[tuple[str, str | None]] = []

    def fake_execute(job, **kwargs):
        calls.append((job.name, kwargs.get("partition_key")))
        return True

    monkeypatch.setattr(
        etl_module.defs,
        "resolve_job_def",
        lambda name: _JOB_DEFS_BY_NAME[name],
    )
    monkeypatch.setattr(etl_module, "_execute", fake_execute)
    return calls


def _run(args: list[str]):
    return CliRunner().invoke(etl, args)


def test_default_runs_both_all_jobs_at_latest(executed):
    result = _run([])
    assert result.exit_code == 0, result.output
    assert executed == [
        ("all_partitioned_metrics_etl", "2026-09-25"),
        ("all_nonpartitioned_metrics_etl", None),
    ]


def test_job_flag_runs_only_that_dataset(executed):
    result = _run(["--job", "eel_hole"])
    assert result.exit_code == 0, result.output
    assert executed == [("eel_hole_metrics_etl", "2026-09-25")]


def test_partitioned_flag_skips_nonpartitioned(executed):
    result = _run(["--partitioned"])
    assert result.exit_code == 0, result.output
    assert executed == [("all_partitioned_metrics_etl", "2026-09-25")]


def test_no_partitioned_flag_runs_only_nonpartitioned(executed):
    result = _run(["--no-partitioned"])
    assert result.exit_code == 0, result.output
    assert executed == [("all_nonpartitioned_metrics_etl", None)]


def test_start_end_loops_over_every_date(executed):
    result = _run(["--job", "eel_hole", "--start", "2026-09-24", "--end", "2026-09-25"])
    assert result.exit_code == 0, result.output
    assert executed == [
        ("eel_hole_metrics_etl", "2026-09-24"),
        ("eel_hole_metrics_etl", "2026-09-25"),
    ]


def test_job_and_partitioned_flag_together_is_a_usage_error(executed):
    result = _run(["--job", "eel_hole", "--partitioned"])
    assert result.exit_code != 0
    assert "cannot be combined" in result.output
    assert executed == []


def test_partition_and_start_together_is_a_usage_error(executed):
    result = _run(["--partition", "2026-09-25", "--start", "2026-09-24"])
    assert result.exit_code != 0
    assert "cannot be combined" in result.output
    assert executed == []


def test_partition_with_no_partitioned_job_is_a_usage_error(executed):
    result = _run(["--no-partitioned", "--partition", "2026-09-25"])
    assert result.exit_code != 0
    assert "no partitioned job is selected" in result.output
    assert executed == []


def test_invalid_partition_is_a_usage_error(executed):
    result = _run(["--job", "eel_hole", "--partition", "1999-01-01"])
    assert result.exit_code != 0
    assert "not a valid partition" in result.output


# --- _execute()'s asset-check surfacing -------------------------------------


def _fake_check(*, passed, severity, description, metadata=None):
    return SimpleNamespace(
        passed=passed,
        severity=severity,
        description=description,
        metadata=metadata or {},
        asset_key=SimpleNamespace(to_user_string=lambda: "some_asset"),
        check_name="some_check",
    )


def _fake_job(checks):
    result = SimpleNamespace(success=True, get_asset_check_evaluations=lambda: checks)
    return Mock(name="job", execute_in_process=Mock(return_value=result))


def test_execute_logs_failed_check_description(caplog):
    job = _fake_job([_fake_check(passed=False, severity="WARN", description="gap")])
    with caplog.at_level(logging.WARNING, logger="usage_metrics"):
        _execute(job)
    assert "gap" in caplog.text


def test_execute_prints_full_report_when_attached(caplog):
    """A check's 'report' metadata is printed in full, not just its one-line description.

    Some checks (eel_hole_event_coverage) attach a full, copy-paste-actionable
    report as metadata because the description alone isn't enough to act on.
    That report needs to show up in this trailing summary -- not just where it
    was first logged mid-run -- so it isn't lost in a long scroll of logs.
    """
    long_report = "EEL-HOLE EVENT COVERAGE -- 2026-09-25\n  preview -- 1898 (85%)"
    job = _fake_job(
        [
            _fake_check(
                passed=False,
                severity="WARN",
                description="coverage gap (non-fatal)",
                metadata={"report": SimpleNamespace(value=long_report)},
            )
        ]
    )
    with caplog.at_level(logging.WARNING, logger="usage_metrics"):
        _execute(job)
    assert long_report in caplog.text


def test_execute_skips_report_line_when_absent(caplog):
    job = _fake_job([_fake_check(passed=False, severity="ERROR", description="broke")])
    with caplog.at_level(logging.WARNING, logger="usage_metrics"):
        _execute(job)
    assert "broke" in caplog.text
    assert "EEL-HOLE" not in caplog.text


def test_execute_fails_the_job_when_a_blocking_check_fails(caplog):
    """A failing blocking check, e.g. a table's schema check, fails the whole job.

    The assets downstream of the table don't run, and the check's description and
    report are still printed at the end, since that is all a reviewer has to go on.
    """
    ran = []

    @asset
    def table():
        ran.append("table")

    @asset_check(asset=table, blocking=True)
    def schema_check():
        return AssetCheckResult(
            passed=False,
            description="table failed its schema",
            metadata={"report": "tls_version  str_matches  2  ['TLSv1.2x']"},
        )

    @asset
    def summary(table):
        ran.append("summary")

    job = Definitions(
        assets=[table, summary],
        asset_checks=[schema_check],
        jobs=[define_asset_job("job")],
    ).resolve_job_def("job")

    with caplog.at_level(logging.WARNING, logger="usage_metrics"):
        succeeded = _execute(job)

    assert not succeeded
    assert ran == ["table"]
    assert "table failed its schema" in caplog.text
    assert "TLSv1.2x" in caplog.text
