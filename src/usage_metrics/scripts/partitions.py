"""The ``usage-metrics partitions`` subcommand.

Expands a start/end partition range into the JSON list of dates that
``load-metrics.yml``'s backfill matrix fans out over.
"""

import json
from datetime import date, timedelta

import click

from usage_metrics.scripts import CONTEXT_SETTINGS


def compute_partitions(start: str | None, end: str | None) -> list[str]:
    """Expand a start/end partition range (inclusive) into a list of ISO dates.

    - Neither given: a single-item list containing ``""`` (meaning "latest").
    - Only one given: that single date, as a one-item list.
    - Both given: every date from ``start`` to ``end``, inclusive.

    Args:
        start: First partition date, in ``YYYY-MM-DD`` form, or ``None``/blank.
        end: Last partition date (inclusive), in ``YYYY-MM-DD`` form, or
            ``None``/blank.

    Returns:
        The list of partition dates to process.
    """
    start = start or None
    end = end or None

    if start and end:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
        if end_date < start_date:
            raise ValueError("end_partition must be on or after start_partition.")
        partitions = []
        current = start_date
        while current <= end_date:
            partitions.append(current.isoformat())
            current += timedelta(days=1)
        return partitions

    return [start or end or ""]


@click.command("partitions", context_settings=CONTEXT_SETTINGS)
@click.option("--start", type=str, default=None, help="First partition date.")
@click.option("--end", type=str, default=None, help="Last partition date.")
def partitions(start: str | None, end: str | None) -> None:
    """Print the JSON list of partitions between --start and --end (inclusive).

    Used by load-metrics.yml to build a backfill matrix. With neither option,
    prints ``[""]`` (meaning "latest"); with only one, prints that single
    partition as a one-item list.
    """
    try:
        result = compute_partitions(start, end)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e
    click.echo(json.dumps(result))
