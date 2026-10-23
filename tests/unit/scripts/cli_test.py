"""Tests for the usage-metrics CLI wiring."""

import subprocess
import sys
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from usage_metrics.scripts.cli import cli


def test_help_lists_subcommands():
    """The top-level group exposes `etl` and `save`."""
    result = CliRunner().invoke(cli, ["-h"])
    assert result.exit_code == 0
    assert "etl" in result.output
    assert "save" in result.output


def test_etl_help_needs_no_credentials():
    """`usage-metrics etl -h` must not require Kaggle creds or load Dagster."""
    result = CliRunner().invoke(cli, ["etl", "-h"])
    assert result.exit_code == 0, result.output


def test_import_does_not_pull_in_kaggle():
    """Importing the CLI must not import the kaggle package (it auths on import)."""
    code = "import usage_metrics.scripts.cli, sys; sys.exit('kaggle' in sys.modules)"
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("args", "target"),
    [
        (["save", "github"], "usage_metrics.scripts.save_github_metrics.save_metrics"),
        (["save", "kaggle"], "usage_metrics.scripts.save_kaggle_metrics.save_metrics"),
        (
            ["save", "zenodo"],
            "usage_metrics.scripts.save_zenodo_metrics.save_zenodo_logs",
        ),
    ],
)
def test_save_subcommands_dispatch(args, target):
    """Each `save` subcommand calls its underlying function."""
    with patch(target) as underlying:
        result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    underlying.assert_called_once()
