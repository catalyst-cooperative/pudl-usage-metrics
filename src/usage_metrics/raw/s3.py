"""Extract data from S3 logs."""

import re
import shutil
from collections.abc import Iterable, Iterator
from compression import zstd
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    Config,
    DailyPartitionsDefinition,
    Failure,
    MaterializeResult,
    Output,
    asset,
    get_dagster_logger,
)
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

from usage_metrics.core.s3 import name_s3_log_columns
from usage_metrics.paths import (
    PUDL_METRICS_S3_LOGS_BUCKET,
    get_gcs_raw_path,
    split_gcs_uri,
)
from usage_metrics.raw.compose import compose_day
from usage_metrics.raw.extract import (
    GCS_EXTRACT_RETRY_POLICY,
    GCSExtractor,
    report_warning,
)

DATASET_NAME = "pudl_s3_logs"
"""Names the local ``raw/`` subdirectory and the compacted artifacts in GCS."""

TRANSFER_DELAY = timedelta(days=1, hours=2)
"""How long after the start of a partition's day its logs are fully in GCS.

A Storage Transfer job copies each day's objects into GCS shortly after 00:00 UTC the
next day. Building an artifact earlier could bake in a partial day.
"""

ZSTD_LEVEL = 9
"""zstd compression level for the compacted artifact.

Levels run from 1 to 22, and the default is 3. These logs compress well, so a higher
level is worth it: at 9 they're 9% smaller than with gzip and faster to read. Level 19
gained little more for far longer compression time.
"""

# Some clients embed raw, unescaped double quotes inside a quoted field --
# e.g. pip (>= 20) sends a User-Agent like
# `"pip/24.3.1 {"ci":null,"cpu":"x86_64",...}"`. That violates CSV's quoting
# rules (an embedded quote must be doubled), so polars refuses to parse the
# line at all ("not properly escaped").
#
# There's no reliable way to *repair* this: any embedded quote is inherently
# ambiguous (that's exactly why CSV requires escaping), and even AWS's own
# Athena RegexSerDe and other widely-used S3-log parsers don't attempt
# to -- they use a `[^"]*` quoted-field pattern that simply fails to match
# these lines, so the row is silently dropped. We do the same: drop the
# (rare) offending line and keep the rest of the day's data, rather than
# guess at its content.
#
# A closing `"` immediately followed by whitespace can only be the real end of
# a quoted field: none of these log's quoted fields (the request line, referer,
# user-agent) legitimately end a piece of content with `"` right before a
# space, and compact JSON never puts a raw space right after a quote either
# (only `:`, `,`, or `}` do -- a string value's closing quote is itself
# followed by more JSON punctuation, not whitespace). So this reliably finds
# each quoted field's true extent regardless of what's inside it; a line only
# needs dropping when a field's content itself still contains a `"`.
_QUOTED_FIELD = re.compile(r'"(.*?)"(?=\s)', re.DOTALL)


# Fail the partition if more than this fraction of its lines had to be dropped
# for embedded quotes. A handful of pip-style lines is expected; a large share
# means the log format changed or a new client is misbehaving, and the data
# loss should be investigated rather than silently accepted.
MAX_DROPPED_LINE_FRACTION = 0.001


def _has_embedded_quotes(line: str) -> bool:
    """Whether any quoted field in this line contains an unescaped quote."""
    return any('"' in field for field in _QUOTED_FIELD.findall(line))


class FusedRecordsError(ValueError):
    """Two log records share one line (a source object lacked a trailing newline)."""


def compacted_location(partition_key: str) -> tuple[str, str]:
    """Get the bucket and object name of a partition's compacted artifact."""
    return split_gcs_uri(f"{get_gcs_raw_path()}/{DATASET_NAME}/{partition_key}.log.zst")


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _read_text(path: Path) -> str:
    """Read a (possibly zstd-compressed) text file, replacing undecodable bytes.

    Bytes that aren't valid UTF-8 become U+FFFD, and stay that way in any data parsed
    from the returned text.
    """
    if path.suffix == ".zst":
        with zstd.open(path, "rt", errors="replace") as f:
            return f.read()
    return path.read_text(errors="replace")


def zstd_compress(src: Path, dest: Path) -> None:
    """Compress ``src`` to ``dest`` with zstd."""
    with src.open("rb") as raw, zstd.open(dest, "wb", level=ZSTD_LEVEL) as out:
        shutil.copyfileobj(raw, out)
        out.write(b"")  # An empty file still gets a valid (empty) frame.


def _record_signature(first_line: bytes) -> bytes:
    """The ``<bucket owner> <bucket> `` prefix that starts every log record."""
    parts = first_line.split(b" ", 2)
    if len(parts) < 3:
        raise FusedRecordsError(f"Not a log record: {first_line[:200]!r}")
    return b" ".join(parts[:2]) + b" "


def zstd_with_guard(src: Path, dest: Path) -> int:
    """Compress ``src`` to ``dest``, checking that each line is exactly one record.

    We may have used GCS ``compose()`` to produce the contents of ``src``.
    ``compose()`` concatenates objects with no separator, so an object lacking a
    trailing newline fuses its last record with the next object's first. A fused
    line contains the record prefix twice.

    Returns:
        The number of lines written.

    Raises:
        FusedRecordsError: If a line doesn't start with the record prefix or
            contains it more than once. ``dest`` is left partially written.
    """
    signature = b""
    lines = 0
    with src.open("rb") as raw, zstd.open(dest, "wb", level=ZSTD_LEVEL) as out:
        for line in raw:
            signature = signature or _record_signature(line)
            lines += 1
            if not line.startswith(signature) or line.count(signature) != 1:
                raise FusedRecordsError(
                    f"Line {lines} of {src} is not exactly one log record: "
                    f"{line[:200]!r}"
                )
            out.write(line)
        out.write(b"")  # An empty file still gets a valid (empty) frame.
    return lines


class S3Extractor(GCSExtractor):
    """Extractor for S3 logs stored in GCS."""

    concatenable_files = True

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = DATASET_NAME
        self.bucket_name = PUDL_METRICS_S3_LOGS_BUCKET
        super().__init__(*args, **kwargs)

    def get_blob_prefix(self, context: AssetExecutionContext) -> str:
        """Filter the bucket listing to this partition's date server-side."""
        return date.fromisoformat(context.partition_key).strftime("%Y-%m-%d")

    def filter_blobs(
        self, context: AssetExecutionContext, blobs: Iterable[storage.Blob]
    ) -> list[storage.Blob]:
        """From all possible files in a bucket, filter to include relevant ones.

        The timestamp in an S3 log file's name is when it was written, so it can hold
        requests made the day before. ``raw_s3_logs`` sorts out which day each
        request belongs to.

        Args:
            context: The Dagster asset execution context
            blobs: the list of all file blobs in the bucket, returned by
                bucket.list_blobs()

        Returns:
            A list of blobs to be downloaded.
        """
        day_start_date_str = context.partition_key
        partition_date = date.fromisoformat(day_start_date_str).strftime("%Y-%m-%d")
        return [
            blob
            for blob in blobs
            if blob.name is not None and blob.name.startswith(partition_date)
        ]

    def load_file(self, file_path: Path) -> pl.DataFrame:
        """Read a (possibly concatenated) day of S3 logs into a dataframe.

        Columns are read as strings; the ``core`` layer names them and coerces
        types. An empty file raises ``pl.exceptions.NoDataError``, which
        ``extract`` handles.
        """
        try:
            return pl.read_csv(
                file_path,
                separator=" ",
                has_header=False,
                infer_schema=False,
            )
        except pl.exceptions.ComputeError as e:
            # On 2026-02-25 a new column was added mid log file, so column-count
            # inference from the first row is wrong. This affects many files that
            # day, so key off the partition rather than identifying each file and
            # force the full 28 columns (short rows are null-padded).
            if self.partition_key == "2026-02-25":
                return pl.read_csv(
                    file_path,
                    separator=" ",
                    has_header=False,
                    infer_schema=False,
                    schema={f"column_{i + 1}": pl.String for i in range(28)},
                    truncate_ragged_lines=True,
                    missing_columns="insert",
                )
            if "not properly escaped" in str(e):
                lines = _read_text(file_path).splitlines(keepends=True)
                kept = [line for line in lines if not _has_embedded_quotes(line)]
                n_dropped = len(lines) - len(kept)
                fraction = n_dropped / len(lines)
                msg = (
                    f"Dropped {n_dropped} of {len(lines)} lines ({fraction:.4%}) with "
                    f"embedded quotes from {file_path.name}."
                )
                if fraction > MAX_DROPPED_LINE_FRACTION:
                    raise ValueError(
                        f"{msg} Exceeds the {MAX_DROPPED_LINE_FRACTION:.4%} limit; "
                        "investigate before accepting this partition."
                    ) from e
                get_dagster_logger().warning(msg)
                return pl.read_csv(
                    "".join(kept).encode(),
                    separator=" ",
                    has_header=False,
                    infer_schema_length=0,
                )
            e.add_note(f"Extraction failed for file: {file_path}")
            raise


S3_PARTITIONS = DailyPartitionsDefinition(start_date="2023-08-16")


class CompactedS3LogsConfig(Config):
    """Run configuration for ``compacted_s3_logs``."""

    rebuild: bool = False
    """Replace an existing artifact instead of reusing it."""


def _combine_day(
    context: AssetExecutionContext, ext: S3Extractor, day: str, *, compose: bool
) -> tuple[Path, int] | None:
    """Download a day's logs into one local file.

    Args:
        day: The ISO date whose log objects to combine.
        compose: Reduce the objects server-side first (up to 1024 per composite),
            so we download far fewer files.

    Returns:
        The combined file and the number of source objects, or ``None`` if the
        day has no objects.
    """
    bucket = ext.gcs_client.bucket(ext.bucket_name)
    if compose:
        blobs, count = compose_day(bucket, day, day, ext.compose_workers)
    else:
        blobs = list(bucket.list_blobs(prefix=day))
        count = len(blobs)
    if not blobs:
        return None
    context.log.info(
        f"Downloading {len(blobs):,} blobs for {count:,} objects of {day}."
    )
    download_dir = ext.get_download_dir()
    paths = ext.get_blobs_from_gcs(blobs, download_dir)
    return ext.combine_files(paths, download_dir / f"{day}.combined"), count


def _build_artifact(
    context: AssetExecutionContext, ext: S3Extractor, day: str
) -> tuple[Path, int] | None:
    """Build a day's ``.log.zst`` locally.

    Returns:
        The artifact and the number of source objects, or ``None`` for an empty day.

    Raises:
        Failure: If the day has source objects, but they hold no records.
    """
    built = _combine_day(context, ext, day, compose=True)
    if built is None:
        return None
    combined, count = built
    if combined.stat().st_size == 0:
        raise Failure(
            description=(
                f"The {count:,} raw log objects for {day} hold no records, so no "
                "compacted log was stored."
            ),
            allow_retries=False,
        )
    dest = combined.parent / f"{day}.log.zst"
    try:
        zstd_with_guard(combined, dest)
    except FusedRecordsError as e:
        context.log.warning(
            f"Composed logs contain fused records ({e}); downloading every object."
        )
        rebuilt = _combine_day(context, ext, day, compose=False)
        if rebuilt is None:
            raise RuntimeError(
                "Downloading every object instead of composing found no files; "
                f"expected {count:,}."
            ) from e
        combined, count = rebuilt
        zstd_compress(combined, dest)
    return dest, count


def _source_count(artifact: storage.Blob) -> int:
    """How many raw log objects an artifact was built from, or 0 if it doesn't say."""
    return int((artifact.metadata or {}).get("source_object_count", 0))


def _ensure_compacted(
    context: AssetExecutionContext, ext: S3Extractor, day: str, *, rebuild: bool
) -> dict[str, str | int | float] | None:
    """Make sure a day's compacted log is in GCS, building it if need be.

    Runs for neighbouring days can build the same day's compacted log at once, so it's
    only stored if what's in GCS hasn't changed since it was looked up. Otherwise the
    log that another run stored first is reused, if it was built from as many raw log
    objects.

    Args:
        day: The ISO date of the logs.
        rebuild: Replace an existing artifact instead of reusing it.

    Returns:
        What was done, as asset metadata, or ``None`` if the day has neither logs nor
        a compacted log.

    Raises:
        Failure: If a rebuild finds fewer raw log objects than the existing artifact
            was built from, or the objects hold no records, or if another run stored
            a log built from a different number of them. Nothing is replaced.
    """
    bucket_name, path = compacted_location(day)
    artifacts = ext.gcs_client.bucket(bucket_name)

    existing = artifacts.get_blob(path)
    if existing is not None and not rebuild:
        return {"action": "reused"}

    built = _build_artifact(context, ext, day)
    if built is None:
        if existing is None:
            return None
        report_warning(
            context,
            f"No S3 logs for {day}, so the rebuild kept the existing compacted log.",
        )
        return {"action": "kept"}
    local, count = built
    if existing is not None and count < _source_count(existing):
        raise Failure(
            description=(
                f"Found {count:,} raw log objects for {day}, but gs://{bucket_name}/"
                f"{path} was built from {_source_count(existing):,}, so it was not "
                "replaced. If it really should be, delete it and run again."
            ),
            allow_retries=False,
        )

    blob = artifacts.blob(path)
    blob.metadata = {
        "source_object_count": str(count),
        "built_at": _utcnow().isoformat(),
    }
    try:
        blob.upload_from_filename(
            str(local),
            content_type="application/zstd",
            # A generation of 0 means there must be nothing there yet.
            if_generation_match=0 if existing is None else existing.generation,
        )
    except PreconditionFailed as e:
        other = artifacts.get_blob(path)
        other_count = 0 if other is None else _source_count(other)
        if other_count != count:
            raise Failure(
                description=(
                    f"Found {count:,} raw log objects for {day}, but another run "
                    f"stored gs://{bucket_name}/{path} while this one was building "
                    f"it, from {other_count:,}. Check the raw logs, then run again "
                    "with --rebuild."
                ),
                allow_retries=False,
            ) from e
        context.log.info(f"Another run stored the compacted log for {day} first.")
        return {"action": "reused"}
    return {
        "action": "built",
        "source_object_count": count,
        "compacted_mb": round((blob.size or 0) / 1e6, 2),
    }


def _day_after(day: str) -> str:
    return (date.fromisoformat(day) + timedelta(days=1)).isoformat()


@asset(
    partitions_def=S3_PARTITIONS,
    tags={"source": "s3"},
    retry_policy=GCS_EXTRACT_RETRY_POLICY,
    kinds={"gcs"},
    output_required=False,
)
def compacted_s3_logs(
    context: AssetExecutionContext, config: CompactedS3LogsConfig
) -> Iterator[MaterializeResult]:
    """Concatenate S3 log objects into one zstd file per day in GCS.

    Publishing a day takes its own logs and the next day's, since requests made late
    in a day are often logged the day after. So this ensures both days have a
    compacted log, and produces nothing until the next day's logs are due in GCS.

    An existing artifact is reused without touching the source bucket, unless a
    rebuild is asked for, which applies to the partition's own day only. Otherwise
    the day's objects are composed server-side, downloaded, compressed and uploaded.

    A day without logs stores nothing and produces nothing, so nothing downstream
    runs, and a later run looks for the logs again. A rebuild that finds no logs keeps
    the existing artifact.

    Raises:
        Failure: If a rebuild finds fewer raw log objects than the existing artifact
            was built from, or the objects hold no records. Nothing is replaced.
    """
    key = context.partition_key
    next_day = _day_after(key)
    ready = datetime.combine(date.fromisoformat(next_day), time(0), UTC)
    ready += TRANSFER_DELAY
    if _utcnow() < ready:
        context.log.info(
            f"Skipping {key}: the logs of {next_day} aren't due in GCS until {ready}."
        )
        return

    ext = S3Extractor()
    own = _ensure_compacted(context, ext, key, rebuild=config.rebuild)
    if own is None:
        report_warning(context, f"No S3 logs for {key}; nothing was written.")
        return
    following = _ensure_compacted(context, ext, next_day, rebuild=False)
    if following is None:
        report_warning(
            context,
            f"No S3 logs for {next_day}, so {key} is published without the requests "
            "that were logged the day after they were made.",
        )
    yield MaterializeResult(
        metadata={
            **own,
            "next_day_action": following["action"] if following else "no logs",
        }
    )


def _read_compacted(ext: S3Extractor, day: str) -> pl.DataFrame | None:
    """Read a day's compacted log, with unnamed columns, or ``None`` if it's empty.

    Raises:
        FileNotFoundError: If the day has no compacted log.
    """
    bucket_name, path = compacted_location(day)
    blob = ext.gcs_client.bucket(bucket_name).get_blob(path)
    if blob is None:
        raise FileNotFoundError(
            f"gs://{bucket_name}/{path} does not exist; "
            "materialize compacted_s3_logs for this partition first."
        )
    local = ext.get_download_dir() / f"{day}.log.zst"
    blob.download_to_filename(str(local))
    ext.partition_key = day
    try:
        return ext.load_file(local)
    except pl.exceptions.NoDataError:
        return None


def _made_on(day: date) -> pl.Expr:
    """Whether a record's request was made on ``day``, going by its raw time field.

    S3 log times are in UTC, look like ``[06/Feb/2019:00:00:38``, and are the third
    field. The column is picked by position, since polars doesn't name the columns the
    same way for every way ``load_file`` reads them.
    """
    return pl.nth(2).str.starts_with(day.strftime("[%d/%b/%Y:")).fill_null(False)


@asset(
    partitions_def=S3_PARTITIONS,
    deps=[compacted_s3_logs],
    tags={"source": "s3"},
    retry_policy=GCS_EXTRACT_RETRY_POLICY,
    output_required=False,
)
def raw_s3_logs(context: AssetExecutionContext) -> Iterator[Output[pd.DataFrame]]:
    """Read the S3 log records that belong to a day, with named columns.

    These are the requests made on that day, from its own compacted log and the next
    day's. Requests from the day before that are in its own log belong to that day's
    partition. The very rare records that are in its log but were made at some other
    time are kept here, since no other partition reads them, and reported.

    Nothing is produced for a day without logs, so nothing downstream runs.
    """
    key = context.partition_key
    day = date.fromisoformat(key)
    next_day = _day_after(key)
    ext = S3Extractor()

    own = _read_compacted(ext, key)
    if own is None:
        report_warning(context, f"No S3 logs for {key}; nothing was written.")
        return
    own = own.filter(~_made_on(day - timedelta(days=1)))
    if stragglers := own.filter(~_made_on(day)).height:
        report_warning(
            context,
            f"{stragglers:,} records in the S3 logs of {key} were made neither that "
            "day nor the day before. They're in core_s3_logs, but not the summaries.",
        )
    frames = [name_s3_log_columns(own.to_pandas(), key)]
    del own

    try:
        following = _read_compacted(ext, next_day)
    except FileNotFoundError:
        context.log.warning(f"There is no compacted log for {next_day}.")
        following = None
    if following is not None:
        following = following.filter(_made_on(day))
        if following.height:
            frames.append(name_s3_log_columns(following.to_pandas(), next_day))

    yield Output(pd.concat(frames, ignore_index=True))
