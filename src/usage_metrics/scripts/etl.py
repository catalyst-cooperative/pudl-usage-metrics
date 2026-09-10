"""The ``usage-metrics etl`` subcommand.

Runs the most recent partition for every job in the usage_metrics Dagster
repository. Run daily by the load-metrics GitHub Action.

Note: Eventually this should be deprecated in favor of a long running Dagster
instance handling schedules and job launching.
"""

import logging
import os
import sys

import click
import coloredlogs

from usage_metrics.etl import defs
from usage_metrics.scripts import CONTEXT_SETTINGS
from usage_metrics.scripts.partitions import compute_partitions

logger = logging.getLogger("usage_metrics")

JOB_ALIASES: dict[str, str] = {
    "s3": "s3_metrics_etl",
    "kaggle": "kaggle_metrics_etl",
    "github_partitioned": "github_partitioned_metrics_etl",
    "github_nonpartitioned": "github_nonpartitioned_metrics_etl",
    "zenodo": "zenodo_metrics_etl",
    "eel_hole": "eel_hole_metrics_etl",
}
"""Short names for the per-source jobs, as accepted by ``--job``."""


def _execute(job, **execute_kwargs) -> bool:
    """Run a job to completion without raising; log and return whether it succeeded."""
    logger.info(f"Starting {job.name}.")
    result = job.execute_in_process(raise_on_error=False, **execute_kwargs)
    # Surface non-passing asset checks at the end of the run so a reviewer sees
    # them without scrolling the Dagster event log -- including WARN checks (e.g.
    # eel_hole_event_coverage) that don't fail the job but flag a coverage gap.
    for check in result.get_asset_check_evaluations():
        if not check.passed:
            level = (
                logging.ERROR
                if str(check.severity).endswith("ERROR")
                else logging.WARNING
            )
            logger.log(
                level,
                f"{job.name}: asset check "
                f"{check.asset_key.to_user_string()}.{check.check_name} "
                f"[{check.severity}] -- {check.description}",
            )
    logger.info(f"{job.name} {'succeeded' if result.success else 'FAILED'}.")
    return result.success


def _resolve_jobs(job_alias: str | None, partitioned: bool | None):
    """Resolve --job/--partitioned/--no-partitioned into the job(s) to run.

    Exactly one of "a specific dataset" or "partitioned-ness" narrows the
    selection -- they're two ways of expressing the same choice, so combining
    them is rejected by the caller before this runs.
    """
    if job_alias:
        return [defs.resolve_job_def(name=JOB_ALIASES[job_alias])]
    if partitioned is True:
        return [defs.resolve_job_def(name="all_partitioned_metrics_etl")]
    if partitioned is False:
        return [defs.resolve_job_def(name="all_nonpartitioned_metrics_etl")]
    return [
        defs.resolve_job_def(name="all_partitioned_metrics_etl"),
        defs.resolve_job_def(name="all_nonpartitioned_metrics_etl"),
    ]


@click.command("etl", context_settings=CONTEXT_SETTINGS)
@click.option(
    "-p",
    "--partition",
    type=str,
    default=None,
    help="A single partition date (YYYY-MM-DD). Mutually exclusive with --start/--end.",
)
@click.option(
    "--start", type=str, default=None, help="First partition date of a range."
)
@click.option("--end", type=str, default=None, help="Last partition date of a range.")
@click.option(
    "--job",
    "job_alias",
    type=click.Choice(sorted(JOB_ALIASES)),
    default=None,
    help="Run only this dataset's job, instead of every partitioned/non-partitioned job.",
)
@click.option(
    "--partitioned/--no-partitioned",
    "partitioned",
    default=None,
    help=(
        "Restrict to only the partitioned or only the non-partitioned jobs. "
        "Default (neither flag) runs both. Not allowed together with --job."
    ),
)
def etl(
    partition: str | None,
    start: str | None,
    end: str | None,
    job_alias: str | None,
    partitioned: bool | None,
):
    """Load the latest partition of every metrics source to Google Cloud Storage."""
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=logger)
    logger.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} storage.")

    if job_alias and partitioned is not None:
        raise click.UsageError(
            "--job cannot be combined with --partitioned/--no-partitioned -- "
            "they're two ways of picking which job(s) to run."
        )
    if partition and (start or end):
        raise click.UsageError("--partition cannot be combined with --start/--end.")

    try:
        dates = compute_partitions(start or partition, end)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e

    job_defs = _resolve_jobs(job_alias, partitioned)
    partitioned_jobs = [j for j in job_defs if j.partitions_def is not None]
    nonpartitioned_jobs = [j for j in job_defs if j.partitions_def is None]

    if dates != [""] and not partitioned_jobs:
        raise click.UsageError(
            "--partition/--start/--end given, but no partitioned job is selected."
        )

    results: dict[str, bool] = {}

    for job in partitioned_jobs:
        assert job.partitions_def is not None, (
            f"{job.name} is expected to have a partitions_def."
        )
        partition_keys = job.partitions_def.get_partition_keys()
        for requested in dates:
            resolved = requested or max(partition_keys)
            if resolved not in partition_keys:
                raise click.BadParameter(
                    f"{resolved!r} is not a valid partition for {job.name} "
                    f"(range: {partition_keys[0]}..{partition_keys[-1]})."
                )
            logger.info(f"Processing partitioned data for {job.name} / {resolved}.")
            results[f"{job.name}:{resolved}"] = _execute(job, partition_key=resolved)

    for job in nonpartitioned_jobs:
        results[job.name] = _execute(job)

    failed = [name for name, succeeded in results.items() if not succeeded]
    if failed:
        logger.error(f"Failed job(s): {', '.join(failed)}")
        sys.exit(1)
