"""Tests for `usage_metrics.scripts.etl`."""

from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from usage_metrics.scripts import etl as etl_module
from usage_metrics.scripts.etl import etl


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
