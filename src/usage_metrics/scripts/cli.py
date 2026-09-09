"""The ``usage-metrics`` command-line interface."""

import logging

import click

from usage_metrics.scripts import (
    CONTEXT_SETTINGS,
    etl,
    save_github_metrics,
    save_kaggle_metrics,
    save_zenodo_metrics,
)


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """Run the PUDL usage-metrics ETL and snapshot metrics from external APIs."""


cli.add_command(etl.etl)


@cli.group(context_settings=CONTEXT_SETTINGS)
def save():
    """Snapshot metrics from external APIs into Google Cloud Storage."""
    logging.basicConfig(level=logging.INFO)


@save.command("github", context_settings=CONTEXT_SETTINGS)
def save_github():
    """Save GitHub traffic metrics."""
    save_github_metrics.save_metrics()


@save.command("kaggle", context_settings=CONTEXT_SETTINGS)
def save_kaggle():
    """Save Kaggle dataset metrics."""
    save_kaggle_metrics.save_metrics()


@save.command("zenodo", context_settings=CONTEXT_SETTINGS)
def save_zenodo():
    """Save Zenodo archive metrics."""
    save_zenodo_metrics.save_zenodo_logs()
