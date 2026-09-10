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

logger = logging.getLogger("usage_metrics")


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


@click.command("etl", context_settings=CONTEXT_SETTINGS)
@click.option("-p", "--partition", type=str, default=None)
def etl(partition: str | None):
    """Load the latest partition of every metrics source to Google Cloud Storage."""
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level="INFO", logger=logger)
    logger.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} storage.")

    partitioned = defs.get_job_def(name="all_partitioned_metrics_etl")
    nonpartitioned = defs.get_job_def(name="all_nonpartitioned_metrics_etl")

    partition_keys = partitioned.partitions_def.get_partition_keys()
    if partition is None:
        partition = max(partition_keys)
    elif partition not in partition_keys:
        raise click.BadParameter(
            f"{partition!r} is not a valid partition "
            f"(range: {partition_keys[0]}..{partition_keys[-1]})."
        )
    logger.info(f"Processing partitioned data for {partition}.")

    # Run both jobs regardless of the other's outcome, then fail if either did.
    results = {
        partitioned.name: _execute(partitioned, partition_key=partition),
        nonpartitioned.name: _execute(nonpartitioned),
    }
    failed = [name for name, succeeded in results.items() if not succeeded]
    if failed:
        logger.error(f"Failed job(s): {', '.join(failed)}")
        sys.exit(1)
