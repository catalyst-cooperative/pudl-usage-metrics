"""Extract data from S3 logs."""

import re
import shutil
from collections.abc import Iterable
from compression import zstd
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    Config,
    DailyPartitionsDefinition,
    MaterializeResult,
    asset,
    get_dagster_logger,
)
from google.cloud import storage

from usage_metrics.paths import (
    PUDL_METRICS_S3_LOGS_BUCKET,
    get_gcs_raw_path,
    split_gcs_uri,
)
from usage_metrics.raw.compose import compose_day
from usage_metrics.raw.extract import GCS_EXTRACT_RETRY_POLICY, GCSExtractor

DATASET_NAME = "pudl_s3_logs"
"""Names the local ``raw/`` subdirectory and the compacted artifacts in GCS."""

TRANSFER_DELAY = timedelta(days=1, hours=2)
"""How long after the start of a partition's day its logs are fully in GCS.

A Storage Transfer job copies each day's objects into GCS shortly after 00:00 UTC the
next day. Building an artifact earlier could bake in a partial day.
"""

ZSTD_LEVEL = 9
"""zstd level for the compacted artifact: smaller and faster to read than gzip, with
level 19 offering little further gain for far more compression time."""

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
    """Read a (possibly zstd-compressed) text file, replacing undecodable bytes."""
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

        Note that the timestamp on the S3 file name corresponds to the end of the window
        in which the logs were produced, meaning that logs can sometimes contain data
        from more than one day. We read these records in based on the file name and use
        the time column as the referent timestamp, so this can look unusual in the
        context of examining a single partition but does not cause any issues in the
        overall complete timeseries analysis.

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
    context: AssetExecutionContext, ext: S3Extractor, *, compose: bool
) -> tuple[Path, int] | None:
    """Download a day's logs into one local file.

    Args:
        compose: Reduce the objects server-side first, so only a few hundred
            composites are downloaded, rather than every object.

    Returns:
        The combined file and the number of source objects, or ``None`` if the
        day has no objects.
    """
    key = context.partition_key
    bucket = ext.gcs_client.bucket(ext.bucket_name)
    prefix = ext.get_blob_prefix(context)
    if compose:
        blobs, count = compose_day(bucket, prefix, key, ext.compose_workers)
    else:
        blobs = ext.filter_blobs(context, bucket.list_blobs(prefix=prefix))
        count = len(blobs)
    if not blobs:
        return None
    context.log.info(
        f"Downloading {len(blobs):,} blobs for {count:,} objects of {key}."
    )
    download_dir = ext.get_download_dir()
    paths = ext.get_blobs_from_gcs(blobs, download_dir)
    return ext.combine_files(paths, download_dir / f"{key}.combined"), count


def _build_artifact(
    context: AssetExecutionContext, ext: S3Extractor
) -> tuple[Path, int] | None:
    """Build the day's ``.log.zst`` locally.

    Returns:
        The artifact and the number of source objects, or ``None`` for an empty day.
    """
    built = _combine_day(context, ext, compose=True)
    if built is None:
        return None
    combined, count = built
    dest = combined.parent / f"{context.partition_key}.log.zst"
    try:
        zstd_with_guard(combined, dest)
    except FusedRecordsError as e:
        context.log.warning(
            f"Composed logs contain fused records ({e}); downloading every object."
        )
        rebuilt = _combine_day(context, ext, compose=False)
        if rebuilt is None:
            raise RuntimeError("Fallback found no files that compose found.") from e
        combined, count = rebuilt
        zstd_compress(combined, dest)
    return dest, count


@asset(
    partitions_def=S3_PARTITIONS,
    tags={"source": "s3"},
    retry_policy=GCS_EXTRACT_RETRY_POLICY,
    kinds={"gcs"},
)
def compacted_s3_logs(
    context: AssetExecutionContext, config: CompactedS3LogsConfig
) -> MaterializeResult:
    """Concatenate a day's S3 log objects into one zstd file in GCS.

    An existing artifact is reused without touching the source bucket. Otherwise
    the day's objects are composed server-side, downloaded, compressed and
    uploaded. An empty day is stored as an empty artifact, so ``raw_s3_logs`` can
    tell "no logs" from "not compacted yet".
    """
    key = context.partition_key
    ext = S3Extractor()
    ext.partition_key = key
    bucket_name, path = compacted_location(key)
    artifacts = ext.gcs_client.bucket(bucket_name)

    if artifacts.get_blob(path) is not None and not config.rebuild:
        return MaterializeResult(metadata={"action": "reused"})

    ready = datetime.combine(date.fromisoformat(key), time(0), UTC) + TRANSFER_DELAY
    if _utcnow() < ready:
        raise RuntimeError(
            f"Logs for {key} may not be fully transferred to GCS until "
            f"{ready.isoformat()}; refusing to build a partial artifact."
        )

    built = _build_artifact(context, ext)
    if built is None:
        context.log.warning(f"No S3 logs found for {key}; recording an empty day.")
        local = ext.get_download_dir() / f"{key}.log.zst"
        local.write_bytes(zstd.compress(b""))
        built = (local, 0)
    local, count = built

    blob = artifacts.blob(path)
    blob.metadata = {
        "source_object_count": str(count),
        "built_at": _utcnow().isoformat(),
    }
    blob.upload_from_filename(str(local), content_type="application/zstd")
    return MaterializeResult(
        metadata={
            "action": "built" if count else "no-data",
            "source_object_count": count,
            "compacted_mb": round((blob.size or 0) / 1e6, 2),
        }
    )


@asset(
    partitions_def=S3_PARTITIONS,
    deps=[compacted_s3_logs],
    tags={"source": "s3"},
    retry_policy=GCS_EXTRACT_RETRY_POLICY,
)
def raw_s3_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Read a day's compacted S3 logs (see ``compacted_s3_logs``) into one DataFrame."""
    key = context.partition_key
    ext = S3Extractor()
    ext.partition_key = key
    bucket_name, path = compacted_location(key)
    blob = ext.gcs_client.bucket(bucket_name).get_blob(path)
    if blob is None:
        raise FileNotFoundError(
            f"gs://{bucket_name}/{path} does not exist; "
            "materialize compacted_s3_logs for this partition first."
        )
    local = ext.get_download_dir() / f"{key}.log.zst"
    blob.download_to_filename(str(local))
    try:
        frame = ext.load_file(local)
    except pl.exceptions.NoDataError:
        context.log.warning(f"No S3 logs for {key}.")
        return pd.DataFrame()
    return frame.to_pandas()
