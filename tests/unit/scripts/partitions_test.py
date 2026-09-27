"""Tests for `usage-metrics partitions`."""

import json

import pytest
from click.testing import CliRunner

from usage_metrics.scripts.cli import cli
from usage_metrics.scripts.partitions import compute_partitions


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (None, None, [""]),
        ("2026-09-25", None, ["2026-09-25"]),
        (None, "2026-09-25", ["2026-09-25"]),
        (
            "2026-09-03",
            "2026-09-06",
            ["2026-09-03", "2026-09-04", "2026-09-05", "2026-09-06"],
        ),
        ("2026-09-25", "2026-09-25", ["2026-09-25"]),
    ],
    ids=[
        "neither_given_means_latest",
        "only_start_given_is_a_single_partition",
        "only_end_given_is_a_single_partition",
        "start_and_end_given_expands_the_range",
        "start_equals_end_is_a_single_day",
    ],
)
def test_compute_partitions(start, end, expected):
    assert compute_partitions(start, end) == expected


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
