"""Tests for `usage_metrics.scripts.etl`."""

import logging
from types import SimpleNamespace

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
_S3_DAYS = ["2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"]
_S3 = _fake_job_def("s3_metrics_etl", _S3_DAYS)
_EEL_HOLE = _fake_job_def("eel_hole_metrics_etl", ["2026-09-24", "2026-09-25"])
_GITHUB_NONPARTITIONED = _fake_job_def("github_nonpartitioned_metrics_etl", None)

_JOB_DEFS_BY_NAME = {
    j.name: j
    for j in [
        _ALL_PARTITIONED,
        _ALL_NONPARTITIONED,
        _S3,
        _EEL_HOLE,
        _GITHUB_NONPARTITIONED,
    ]
}


@pytest.fixture
def executed(monkeypatch):
    """Stub out job resolution and execution; record (job name, kwargs) of each run."""
    calls: list[tuple[str, dict]] = []

    def fake_execute(job, **kwargs):
        calls.append((job.name, kwargs))
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


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        pytest.param(
            [],
            [
                ("all_partitioned_metrics_etl", "2026-09-25"),
                ("all_nonpartitioned_metrics_etl", None),
            ],
            id="default_runs_everything_at_latest",
        ),
        pytest.param(
            ["--job", "eel_hole"],
            [("eel_hole_metrics_etl", "2026-09-25")],
            id="one_job",
        ),
        pytest.param(
            ["--partitioned"],
            [("all_partitioned_metrics_etl", "2026-09-25")],
            id="partitioned_only",
        ),
        pytest.param(
            ["--no-partitioned"],
            [("all_nonpartitioned_metrics_etl", None)],
            id="nonpartitioned_only",
        ),
        pytest.param(
            ["--job", "eel_hole", "--start", "2026-09-24", "--end", "2026-09-25"],
            [
                ("eel_hole_metrics_etl", "2026-09-24"),
                ("eel_hole_metrics_etl", "2026-09-25"),
            ],
            id="start_to_end",
        ),
        pytest.param(
            ["--job", "s3", "--window", "3"],
            [("s3_metrics_etl", day) for day in _S3_DAYS[-3:]],
            id="window_ending_at_latest",
        ),
        pytest.param(
            ["--job", "s3", "--window", "2", "--partition", "2026-09-24"],
            [("s3_metrics_etl", "2026-09-23"), ("s3_metrics_etl", "2026-09-24")],
            id="window_ending_at_partition",
        ),
    ],
)
def test_selects_jobs_and_partitions(executed, args, expected):
    """The options pick which jobs run, and for which dates, oldest first."""
    result = _run(args)
    assert result.exit_code == 0, result.output
    assert [(name, kw.get("partition_key")) for name, kw in executed] == expected


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(
            ["--job", "eel_hole", "--partitioned"], id="job_with_partitioned_flag"
        ),
        pytest.param(
            ["--partition", "2026-09-25", "--start", "2026-09-24"],
            id="partition_with_start",
        ),
        pytest.param(
            ["--no-partitioned", "--partition", "2026-09-25"],
            id="partition_with_no_partitioned_job",
        ),
        pytest.param(
            ["--job", "eel_hole", "--partition", "1999-01-01"], id="unknown_partition"
        ),
        pytest.param(
            ["--job", "s3", "--window", str(len(_S3_DAYS) + 1)],
            id="window_before_first_partition",
        ),
        pytest.param(["--window", "2"], id="window_without_a_job"),
        pytest.param(
            ["--job", "eel_hole", "--window", "2"], id="window_with_another_job"
        ),
        pytest.param(["--rebuild"], id="rebuild_without_a_job"),
        pytest.param(["--job", "eel_hole", "--rebuild"], id="rebuild_with_another_job"),
        pytest.param(
            ["--job", "s3", "--window", "2", "--start", "2026-09-24"],
            id="window_with_start",
        ),
    ],
)
def test_usage_errors_run_nothing(executed, args):
    result = _run(args)
    assert result.exit_code == 2, result.output
    assert executed == []


def test_rebuild_passes_run_config_to_the_job(executed):
    result = _run(["--job", "s3", "--rebuild"])
    assert result.exit_code == 0, result.output
    assert executed == [
        (
            "s3_metrics_etl",
            {
                "partition_key": "2026-09-25",
                "run_config": etl_module.REBUILD_RUN_CONFIG,
            },
        )
    ]


# --- _execute()'s asset-check surfacing -------------------------------------


def test_execute_fails_the_job_when_a_blocking_check_fails(caplog):
    """A failing blocking check, e.g. a table's schema check, fails the whole job.

    The check's description and report are printed at the end, since that is all a
    reviewer has to go on.
    """

    @asset
    def table():
        pass

    @asset_check(asset=table, blocking=True)
    def schema_check():
        return AssetCheckResult(
            passed=False,
            description="table failed its schema",
            metadata={"report": "tls_version  str_matches  2  ['TLSv1.2x']"},
        )

    job = Definitions(
        assets=[table],
        asset_checks=[schema_check],
        jobs=[define_asset_job("job")],
    ).resolve_job_def("job")

    with caplog.at_level(logging.WARNING, logger="usage_metrics"):
        succeeded = _execute(job)

    assert not succeeded
    assert "table failed its schema" in caplog.text
    assert "TLSv1.2x" in caplog.text
