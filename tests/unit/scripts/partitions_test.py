"""Tests for `usage-metrics partitions`."""

import json

import pytest
from click.testing import CliRunner

from usage_metrics.scripts.cli import cli
from usage_metrics.scripts.partitions import compute_partitions


def test_neither_given_means_latest():
    assert compute_partitions(None, None) == [""]


def test_only_start_given_is_a_single_partition():
    assert compute_partitions("2026-09-25", None) == ["2026-09-25"]


def test_only_end_given_is_a_single_partition():
    assert compute_partitions(None, "2026-09-25") == ["2026-09-25"]


def test_start_and_end_given_expands_the_range():
    assert compute_partitions("2026-09-03", "2026-09-06") == [
        "2026-09-03",
        "2026-09-04",
        "2026-09-05",
        "2026-09-06",
    ]


def test_start_equals_end_is_a_single_day():
    assert compute_partitions("2026-09-25", "2026-09-25") == ["2026-09-25"]


def test_end_before_start_raises():
    with pytest.raises(ValueError, match="end_partition must be on or after"):
        compute_partitions("2026-09-25", "2026-09-01")


def test_cli_prints_json():
    result = CliRunner().invoke(
        cli, ["partitions", "--start", "2026-09-03", "--end", "2026-09-04"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == ["2026-09-03", "2026-09-04"]


def test_cli_rejects_end_before_start():
    result = CliRunner().invoke(
        cli, ["partitions", "--start", "2026-09-25", "--end", "2026-09-01"]
    )
    assert result.exit_code != 0
