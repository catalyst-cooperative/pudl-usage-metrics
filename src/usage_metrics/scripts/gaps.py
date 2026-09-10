"""The ``usage-metrics gaps`` subcommand.

For each partitioned metrics source, compare the daily partitions that have raw
data in the source bucket against the processed parquet partitions in the metrics
bucket, and report the dates where raw data exists but no processed output does
(a failed run, or a day that was never processed).

The per-source breakdown goes to stderr; the deduplicated, sorted list of dates
to re-run goes to stdout, so it can be fed straight to ``workflow_dispatch``:

    for d in $(usage-metrics gaps); do
        gh workflow run load-metrics.yml -f partition="$d"
    done

``raw_exists`` checks for a raw object dated exactly ``D``. S3 and eel-hole name
their objects by the day they belong to, so the check is exact there. Zenodo and
GitHub "views" process a trailing 7-day window, so for those this may under-
report gaps at the edges — good enough for finding holes to backfill.
"""

import concurrent.futures
import logging
import os
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import click
from google.cloud import storage

from usage_metrics.scripts import CONTEXT_SETTINGS

logger = logging.getLogger("usage_metrics")

PARTITION_START = date(2023, 8, 16)
"""First daily partition, matching ``DailyPartitionsDefinition`` across the assets."""

METRICS_BUCKET = os.environ.get("GCS_BUCKET", "metrics.catalyst.coop")
ARCHIVES_BUCKET = "pudl-usage-metrics-archives.catalyst.coop"

_PARQUET_PARTITION = re.compile(r"/(\d{4}-\d{2}-\d{2})--\d{4}-\d{2}-\d{2}\.parquet$")


@dataclass(frozen=True)
class Source:
    """A partitioned metrics source and where to find its raw and processed data."""

    name: str
    processed_asset: str
    """Asset name whose per-partition parquet under ``METRICS_BUCKET`` stands in
    for "this partition was processed"."""
    raw_bucket: str
    raw_prefix: Callable[[date], str]
    """``date`` -> a blob-name prefix that exists iff that day has raw data."""

    def processed_partitions(self, client: storage.Client) -> set[date]:
        """Dates for which a processed parquet exists in the metrics bucket."""
        found: set[date] = set()
        blobs = client.bucket(METRICS_BUCKET).list_blobs(
            prefix=f"{self.processed_asset}/"
        )
        for blob in blobs:
            if match := _PARQUET_PARTITION.search(blob.name):
                found.add(date.fromisoformat(match.group(1)))
        return found

    def raw_exists(self, client: storage.Client, day: date) -> bool:
        """Whether the source bucket holds any raw object for ``day``."""
        blobs = client.bucket(self.raw_bucket).list_blobs(
            prefix=self.raw_prefix(day), max_results=1
        )
        return next(iter(blobs), None) is not None


SOURCES: list[Source] = [
    Source(
        "s3",
        "core_s3_logs",
        "pudl-s3-logs.catalyst.coop",
        lambda d: d.isoformat(),
    ),
    Source(
        "eel_hole",
        "core_eel_hole_hits",
        "pudl-viewer-logs.catalyst.coop",
        lambda d: f"run.googleapis.com/stdout/{d:%Y/%m/%d}/",
    ),
    Source(
        "kaggle",
        "core_kaggle_logs",
        ARCHIVES_BUCKET,
        lambda d: f"kaggle/{d.isoformat()}.json",
    ),
    Source(
        "github",
        "core_github_clones",
        ARCHIVES_BUCKET,
        lambda d: f"github/clones/{d.isoformat()}.json",
    ),
    Source(
        "zenodo",
        "core_zenodo_logs",
        ARCHIVES_BUCKET,
        lambda d: f"zenodo/{d.isoformat()}-",
    ),
]


def _daterange(start: date, end: date) -> Iterator[date]:
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


def find_gaps(
    source: Source,
    client: storage.Client,
    start: date,
    end: date,
    check_raw: bool = True,
) -> list[date]:
    """Dates in ``[start, end]`` with no processed parquet (and, by default, raw data)."""
    processed = source.processed_partitions(client)
    missing = [day for day in _daterange(start, end) if day not in processed]
    if not check_raw or not missing:
        return missing
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        has_raw = pool.map(lambda day: source.raw_exists(client, day), missing)
    return [day for day, raw in zip(missing, has_raw, strict=True) if raw]


@click.command("gaps", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--source",
    "selected",
    multiple=True,
    type=click.Choice([s.name for s in SOURCES]),
    help="Only check these sources (repeatable; default: all).",
)
@click.option(
    "--start",
    type=click.DateTime(["%Y-%m-%d"]),
    default=PARTITION_START.isoformat(),
    show_default=True,
    help="First partition date to check.",
)
@click.option(
    "--end",
    type=click.DateTime(["%Y-%m-%d"]),
    default=None,
    help="Last partition date to check (default: yesterday).",
)
@click.option(
    "--check-raw/--no-check-raw",
    default=True,
    show_default=True,
    help="Only report missing partitions that actually have raw data to process.",
)
def gaps(
    selected: tuple[str, ...],
    start: click.DateTime,
    end: click.DateTime | None,
    check_raw: bool,
) -> None:
    """List partitions that have raw data but no processed output."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    client = storage.Client()
    start_date = start.date()
    end_date = end.date() if end else datetime.now(UTC).date() - timedelta(days=1)
    sources = [s for s in SOURCES if not selected or s.name in selected]

    all_gaps: set[date] = set()
    for source in sources:
        gap_days = find_gaps(source, client, start_date, end_date, check_raw)
        all_gaps.update(gap_days)
        click.echo(
            f"{source.name}: {len(gap_days)} missing partition(s)"
            + (" with raw data" if check_raw else ""),
            err=True,
        )
        for day in gap_days:
            click.echo(f"  {day.isoformat()}", err=True)

    click.echo(f"\n{len(all_gaps)} distinct date(s) to backfill:", err=True)
    for day in sorted(all_gaps):
        click.echo(day.isoformat())
