"""The ``usage-metrics`` command-line interface.

The ``save_*`` implementations are imported lazily inside each command so that
running ``usage-metrics etl`` (or any other subcommand) doesn't pull in the
Kaggle client, which authenticates on import and hard-fails when no Kaggle
credentials are set.
"""

import logging

import click

from usage_metrics.scripts import CONTEXT_SETTINGS, etl


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
    from usage_metrics.scripts import save_github_metrics

    save_github_metrics.save_metrics()


@save.command("kaggle", context_settings=CONTEXT_SETTINGS)
def save_kaggle():
    """Save Kaggle dataset metrics."""
    from usage_metrics.scripts import save_kaggle_metrics

    save_kaggle_metrics.save_metrics()


@save.command("zenodo", context_settings=CONTEXT_SETTINGS)
def save_zenodo():
    """Save Zenodo archive metrics."""
    from usage_metrics.scripts import save_zenodo_metrics

    save_zenodo_metrics.save_zenodo_logs()
