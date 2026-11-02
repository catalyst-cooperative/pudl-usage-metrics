"""Extract data from S3 logs."""

import re
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
"""Names the dataset's local ``raw/`` subdirectory and its compacted artifacts in GCS."""

TRANSFER_DELAY = timedelta(days=1, hours=2)
"""How long after the start of a partition's day its logs are fully in GCS.

A Storage Transfer job copies the previous day's S3 log objects into GCS at
00:00 UTC (measured on four days, ~328k objects: all had arrived by 01:00 UTC on
the following day), so
a build attempted before ``D + 1 day + 2 h`` could bake in a partial day."""

ZSTD_LEVEL = 9
"""zstd compression level for the compacted artifact.

Measured on a real 2M-line, 1.1 GB day: level 9 is 9% smaller than gzip -6
(176 vs 195 MB) while compressing faster (3.8 vs 4.9 s) and reading about twice
as fast in polars. Level 19 saves only 3% more for ~55x the compression time."""

_LINES_PER_WRITE = 50_000

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


def _drop_lines_with_embedded_quotes(text: str) -> str:
    """Drop any line containing a field with unescaped embedded quotes."""
    return "".join(
        line
        for line in text.splitlines(keepends=True)
        if not any('"' in field for field in _QUOTED_FIELD.findall(line))
    )


class FusedRecordsError(ValueError):
    """Two log records were joined on one line (a source object lacked a newline)."""


def compacted_location(partition_key: str) -> tuple[str, str]:
    """Get the bucket and object name of a partition's compacted artifact.

    Artifacts are ``<date>.log.zst`` files under ``get_gcs_raw_path()``.
    """
    return split_gcs_uri(f"{get_gcs_raw_path()}/{DATASET_NAME}/{partition_key}.log.zst")


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _read_text(path: Path) -> str:
    """Read a (possibly zstd-compressed) text file, replacing undecodable bytes."""
    if path.suffix == ".zst":
        with zstd.open(path, "rt", errors="replace") as f:
            return f.read()
    return path.read_text(errors="replace")


def zstd_with_guard(src: Path, dest: Path, *, guard: bool = True) -> int:
    """Compress ``src`` to ``dest`` with zstd, checking that no two records share a line.

    ``compose()`` concatenates objects without a separator, so a source object
    lacking a trailing newline would fuse its last record with the next
    object's first. Every S3 access-log record starts with the same
    ``<bucket owner> <bucket> `` prefix, taken from the first line, so a fused
    line contains that prefix twice.

    Args:
        src: The concatenated log text.
        dest: Where to write the compressed copy.
        guard: Set False to skip the check (for input already newline-padded).

    Returns:
        The number of lines written.

    Raises:
        FusedRecordsError: If a line doesn't start with the record prefix or
            contains it more than once. ``dest`` is left partially written.
    """
    signature = b""
    lines = 0
    batch: list[bytes] = []
    with src.open("rb") as raw, zstd.open(dest, "wb", level=ZSTD_LEVEL) as out:
        for line in raw:
            if guard:
                if not signature:
                    owner, bucket, _ = line.split(b" ", 2)
                    signature = owner + b" " + bucket + b" "
                if not line.startswith(signature) or line.count(signature) != 1:
                    raise FusedRecordsError(
                        f"Line {lines + 1} of {src} is not exactly one log record: "
                        f"{line[:200]!r}"
                    )
            batch.append(line)
            lines += 1
            if len(batch) >= _LINES_PER_WRITE:
                out.write(b"".join(batch))
                batch.clear()
        out.write(b"".join(batch))
    return lines


class S3Extractor(GCSExtractor):
    """Extractor for S3 logs stored in GCS."""

    concatenable_files = True

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = DATASET_NAME
        self.bucket_name = PUDL_METRICS_S3_LOGS_BUCKET
        super().__init__(*args, **kwargs)
        # Reduce the day's objects server-side before downloading. Turned off
        # to fall back to downloading every object when the composed bytes
        # fail the fused-record check.
        self.use_compose = True

    def download_gcs_blobs(
        self, context: AssetExecutionContext, download_dir: Path
    ) -> list[Path]:
        """Download the partition's logs, composing them server-side first.

        A heavy day is ~176k tiny objects; ``compose`` reduces them inside GCS
        to a few hundred, which are all that gets downloaded.
        """
        if not self.use_compose:
            return super().download_gcs_blobs(context, download_dir)
        bucket = self.gcs_client.bucket(self.bucket_name)
        blobs, self.source_object_count = compose_day(
            bucket,
            self.get_blob_prefix(context),
            context.partition_key,
            self.compose_workers,
        )
        context.log.info(
            f"Composed {self.source_object_count:,} objects from {self.bucket_name} "
            f"into {len(blobs):,}."
        )
        return self.get_blobs_from_gcs(blobs, download_dir, context)

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
                infer_schema_length=0,
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
                    infer_schema_length=0,
                    schema={f"column_{i + 1}": pl.String for i in range(28)},
                    truncate_ragged_lines=True,
                    missing_columns="insert",
                )
            if "not properly escaped" in str(e):
                cleaned = _drop_lines_with_embedded_quotes(_read_text(file_path))
                return pl.read_csv(
                    cleaned.encode(),
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
    """Rebuild the artifact even if one exists (e.g. after a partial-day build)."""


def _build_artifact(
    context: AssetExecutionContext, ext: S3Extractor
) -> tuple[Path, int] | None:
    """Build the day's ``.log.zst`` locally; return ``(path, source object count)``.

    Returns ``None`` for an empty day.
    """
    built = ext.build_combined_file(context)
    if built is None:
        return None
    combined, count = built
    dest = combined.parent / f"{context.partition_key}.log.zst"
    try:
        zstd_with_guard(combined, dest)
    except FusedRecordsError as e:
        context.log.warning(
            f"Composed logs contain fused records ({e}); falling back to "
            "downloading every object."
        )
        ext.use_compose = False
        rebuilt = ext.build_combined_file(context)
        assert rebuilt is not None, "Fallback found no files the compose path found."
        combined, count = rebuilt
        zstd_with_guard(combined, dest, guard=False)
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
    """Concatenate a day's S3 log objects into one zstd-compressed file in GCS, once.

    An existing artifact is reused without touching the source bucket. Otherwise
    the day's ~100k+ tiny objects are reduced server-side with GCS ``compose``,
    the few resulting objects are downloaded, and the concatenation is compressed
    and uploaded. An empty day is recorded as an empty artifact so that
    ``raw_s3_logs`` can tell "no logs" from "not compacted yet".
    """
    key = context.partition_key
    ext = S3Extractor()
    ext.partition_key = key
    bucket_name, path = compacted_location(key)
    artifacts = ext.gcs_client.bucket(bucket_name)

    existing = artifacts.get_blob(path)
    if existing is not None and not config.rebuild:
        return MaterializeResult(
            metadata={
                "action": "reused",
                "source_object_count": int(
                    (existing.metadata or {}).get("source_object_count", -1)
                ),
            }
        )

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
    if (blob.metadata or {}).get("source_object_count") == "0":
        context.log.warning(f"No S3 logs for {key}.")
        return pd.DataFrame()

    local = ext.get_download_dir() / f"{key}.log.zst"
    blob.download_to_filename(str(local))
    try:
        frame = ext.load_file(local)
    except pl.exceptions.NoDataError:
        context.log.warning(f"{path} contains no data.")
        return pd.DataFrame()
    return frame.to_pandas()
