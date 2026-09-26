"""Extract data from S3 logs."""

import re
from datetime import date
from pathlib import Path

import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    asset,
)
from google.api_core.page_iterator import HTTPIterator
from google.cloud import storage

from usage_metrics.raw.extract import GCS_EXTRACT_RETRY_POLICY, GCSExtractor

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


class S3Extractor(GCSExtractor):
    """Extractor for S3 logs stored in GCS."""

    concatenable_files = True

    def __init__(self, *args, **kwargs):
        """Initialize the extractor."""
        self.dataset_name = "pudl_s3_logs"
        self.bucket_name = "pudl-s3-logs.catalyst.coop"
        super().__init__(*args, **kwargs)

    def get_blob_prefix(self, context: AssetExecutionContext) -> str:
        """Filter the bucket listing to this partition's date server-side."""
        return date.fromisoformat(context.partition_key).strftime("%Y-%m-%d")

    def filter_blobs(
        self, context: AssetExecutionContext, blobs: HTTPIterator
    ) -> list[storage.Blob]:
        """From all possible files in a bucket, filter to include relevant ones.

        Note that the timestamp on the S3 file name corresponds to the end of the window
        in which the logs were produced, meaning that logs can sometimes contain data
        from more than one day. We read these records in based on the file name
        and use the time column as the referent timestamp, so this can look unusual in
        the context of examining a single partition but does not cause any issues in
        the overall complete timeseries analysis.

        Args:
            context: The Dagster asset execution context
            blobs: the list of all file blobs in the bucket, returned by bucket.list_blobs()

        Returns:
            A list of blobs to be downloaded.
        """
        day_start_date_str = context.partition_key
        partition_date = date.fromisoformat(day_start_date_str).strftime("%Y-%m-%d")
        return [blob for blob in blobs if blob.name.startswith(partition_date)]

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
                )
            if "not properly escaped" in str(e):
                cleaned = _drop_lines_with_embedded_quotes(
                    file_path.read_text(errors="replace")
                )
                return pl.read_csv(
                    cleaned.encode(),
                    separator=" ",
                    has_header=False,
                    infer_schema_length=0,
                )
            e.add_note(f"Extraction failed for file: {file_path}")
            raise


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-16"),
    tags={"source": "s3"},
    retry_policy=GCS_EXTRACT_RETRY_POLICY,
)
def raw_s3_logs(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract S3 logs from sub-daily files and return one daily DataFrame."""
    return S3Extractor().extract(context)
