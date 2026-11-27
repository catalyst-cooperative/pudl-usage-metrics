"""Tests for `usage-metrics partitions`."""

import json
from datetime import date

import pytest
from click.testing import CliRunner

from usage_metrics.scripts import partitions as partitions_module
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
    """The list of partitions is the latest, a single day, or every day from start to end."""
    assert compute_partitions(start, end) == expected


def test_end_before_start_raises():
    """An end date before the start date raises ValueError."""
    with pytest.raises(ValueError, match="end_partition must be on or after"):
        compute_partitions("2026-09-25", "2026-09-01")


def test_cli_prints_json():
    """`usage-metrics partitions` prints the partitions as a JSON list."""
    result = CliRunner().invoke(
        cli, ["partitions", "--start", "2026-09-03", "--end", "2026-09-04"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == ["2026-09-03", "2026-09-04"]


def test_cli_rejects_end_before_start():
    """`usage-metrics partitions` exits with an error if the end is before the start."""
    result = CliRunner().invoke(
        cli, ["partitions", "--start", "2026-09-25", "--end", "2026-09-01"]
    )
    assert result.exit_code != 0


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        pytest.param(
            ["--start", "", "--end", ""],
            ["2026-02-27", "2026-02-28", "2026-03-01"],
            id="no_dates_means_the_last_complete_days",
        ),
        pytest.param(
            ["--start", "2025-09-25", "--end", ""],
            ["2025-09-25"],
            id="a_given_date_wins",
        ),
    ],
)
def test_cli_recent(mocker, args, expected):
    """The scheduled workflow passes blank dates, and today isn't complete."""
    mocker.patch.object(
        partitions_module, "_today", autospec=True, return_value=date(2026, 3, 2)
    )

    result = CliRunner().invoke(cli, ["partitions", *args, "--recent", "3"])

    assert json.loads(result.output) == expected
