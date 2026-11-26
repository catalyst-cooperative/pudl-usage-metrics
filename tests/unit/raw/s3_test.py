"""Tests for the S3 log extractor."""

from collections.abc import Iterator
from compression import zstd
from datetime import UTC, datetime
from typing import cast

import pandas as pd
import polars as pl
import pytest
from dagster import MaterializeResult, Output, build_asset_context

from usage_metrics.raw import s3 as s3_module
from usage_metrics.raw.s3 import (
    CompactedS3LogsConfig,
    FusedRecordsError,
    S3Extractor,
    _has_embedded_quotes,
    compacted_location,
    compacted_s3_logs,
    raw_s3_logs,
    zstd_with_guard,
)

BUCKET = "pudl-s3-logs.catalyst.coop"
DAY = "2024-06-15"

N_COLUMNS_V1 = 27
"""Columns parsed from logs written before February 2026, which have 26 fields.

The timestamp is in square brackets rather than quotes, so splitting on spaces puts its
time and UTC offset in separate columns. ``core_s3_logs`` joins them again.
"""
N_COLUMNS_V2 = 28
"""Columns since ``aws_region`` was added."""

PIP_AGENT = 'pip/24.3.1 {"ci":null,"openssl_version":"OpenSSL 3.0.2"}'
"""A User-Agent with unescaped quotes inside a quoted field."""


def log_line(
    agent: str = "curl/8.0", *, ip: str = "198.51.100.1", requester: str = "-"
) -> str:
    """One S3 access-log record with ``N_COLUMNS_V1`` columns."""
    return (
        f"owner bkt [19/Sep/2026:00:00:00 +0000] {ip} {requester} RID "
        'REST.GET.OBJECT k "GET /k HTTP/1.1" 200 - 100 200 5 4 "-" '
        f'"{agent}" - hid SigV4 ECDHE AuthHeader host TLSv1.2 - -\n'
    )


# In the first of these, the bucket owner appears a second time, as the account that
# made the request.
RECORD_A = log_line(requester="owner").encode()
RECORD_B = log_line(ip="198.51.100.2").encode()


# --- filter_blobs ---------------------------------------------------------


def test_filter_blobs_keeps_only_matching_date(filtered_blob_names):
    """filter_blobs keeps blobs whose name starts with the partition date."""
    kept = filtered_blob_names(
        S3Extractor,
        "2024-06-15",
        ["2024-06-15-00-00-00-a", "2024-06-16-00-00-00-b", "2024-06-15"],
    )
    assert kept == ["2024-06-15", "2024-06-15-00-00-00-a"]


# --- load_file -----------------------------------------------------------


@pytest.fixture
def write_zst(tmp_path):
    """Return a function that writes log bytes to a zstd file, like the artifact."""

    def _write(data: bytes):
        path = tmp_path / "day.log.zst"
        path.write_bytes(zstd.compress(data))
        return path

    return _write


@pytest.mark.parametrize(
    ("subdir", "partition_key", "n_columns"),
    [
        ("normal_v1", None, N_COLUMNS_V1),
        ("normal_v2", None, N_COLUMNS_V2),
        # On 2026-02-25 the width changed mid-file, so it's forced to the new one.
        ("ragged", "2026-02-25", N_COLUMNS_V2),
    ],
)
def test_load_file_reads_every_schema_era(
    write_zst, s3_fixture_blobs, subdir, partition_key, n_columns
):
    """Every record is read, with the column count of its schema era."""
    data = b"".join(s3_fixture_blobs(subdir).values())
    ext = S3Extractor()
    ext.partition_key = partition_key
    df = ext.load_file(write_zst(data))
    assert df.shape == (data.count(b"\n"), n_columns)


def test_load_file_ragged_other_partition_raises_with_note(write_zst, s3_fixture_blobs):
    """A ragged file outside 2026-02-25 raises, annotated with the path."""
    ext = S3Extractor()
    ext.partition_key = "2025-01-01"
    path = write_zst(b"".join(s3_fixture_blobs("ragged").values()))
    with pytest.raises(pl.exceptions.ComputeError) as excinfo:
        ext.load_file(path)
    assert excinfo.value.__notes__


# --- _has_embedded_quotes ---------------------------------------------------


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        pytest.param(
            '"GET /k HTTP/1.1" 200 "-" "Mozilla/5.0 (X11; Linux)" - hid\n',
            False,
            id="well_formed_fields",
        ),
        pytest.param(
            '"GET /k HTTP/1.1" 200 "-" "pip/24.3.1 {"ci":null,"cpu":"x86_64"}" - hid\n',
            True,
            id="pip_style_embedded_json_in_user_agent",
        ),
        pytest.param(
            '"GET /k HTTP/1.1" 200 "https://x.com/?q="weird"" "Mozilla/5.0" - hid\n',
            True,
            id="embedded_quote_in_referer_not_just_user_agent",
        ),
        pytest.param(
            '"GET /k HTTP/1.1" 200 "-" "" - hid\n',
            False,
            id="empty_quoted_field",
        ),
        pytest.param("- - - - -\n", False, id="no_quoted_fields_at_all"),
    ],
)
def test_has_embedded_quotes(line, expected):
    """Only lines with a quote inside a quoted field are flagged."""
    assert _has_embedded_quotes(line) is expected


def test_load_file_drops_lines_with_embedded_quotes(write_zst):
    """A few lines with unescaped quotes are dropped, and the rest of the day parses.

    Regression test for the "not properly escaped" CSV error that failed whole
    partitions in the 2026-09 backfill (e.g. 2026-09-19). pip's User-Agent embeds raw
    JSON, including a space inside a string value, within a quoted field.
    """
    bad = log_line(PIP_AGENT, ip="198.51.100.9")
    path = write_zst((bad + log_line() * 9999).encode())  # 0.01% bad
    df = S3Extractor().load_file(path)
    assert df.shape == (9999, N_COLUMNS_V1)
    assert df["column_4"].unique().to_list() == ["198.51.100.1"]


def test_load_file_fails_when_too_many_lines_dropped(write_zst):
    """Dropping more than MAX_DROPPED_LINE_FRACTION of lines fails the partition."""
    path = write_zst((log_line(PIP_AGENT) * 5 + log_line() * 995).encode())  # 0.5%
    with pytest.raises(ValueError):
        S3Extractor().load_file(path)


# --- zstd_with_guard -----------------------------------------------------------


@pytest.mark.parametrize(
    "records",
    [[], [RECORD_A, RECORD_B]],
    ids=["empty_file", "records"],
)
def test_zstd_with_guard_round_trips(tmp_path, records):
    """Well-formed records are compressed unchanged, and the line count returned.

    The bucket owner can appear in the body of a record without failing the
    record-fusion check.
    """
    src = tmp_path / "src"
    src.write_bytes(b"".join(records))
    dest = tmp_path / "dest.zst"
    assert zstd_with_guard(src, dest) == len(records)
    assert zstd.decompress(dest.read_bytes()) == b"".join(records)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(RECORD_A.rstrip(b"\n") + RECORD_B, id="fused_records"),
        pytest.param(b"\n", id="malformed_first_line"),
        pytest.param(
            RECORD_A + b"fragment" + RECORD_B,
            id="record_after_a_fragment",
        ),
    ],
)
def test_zstd_with_guard_rejects_bad_lines(tmp_path, data):
    """Every line must be exactly one record, e.g. none fused by ``compose()``."""
    src = tmp_path / "src"
    src.write_bytes(data)
    with pytest.raises(FusedRecordsError):
        zstd_with_guard(src, tmp_path / "dest.zst")


# --- compacted_s3_logs / raw_s3_logs assets -------------------------------------


@pytest.fixture
def fake_s3_gcs(fake_gcs, patch_download_many, download_dir, monkeypatch):
    """Factory: a fake client (source + artifact buckets) wired into the assets."""

    monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", "gs://test-outputs")

    def _make(source_blobs: dict[str, bytes]):
        client = fake_gcs({BUCKET: source_blobs, compacted_location(DAY)[0]: {}})
        monkeypatch.setattr(
            s3_module, "S3Extractor", lambda *a, **k: S3Extractor(client=client)
        )
        return client

    return _make


def run_compacted_asset(rebuild: bool = False) -> dict:
    """Run ``compacted_s3_logs`` for ``DAY`` and return its metadata."""
    result = compacted_s3_logs(
        build_asset_context(partition_key=DAY), CompactedS3LogsConfig(rebuild=rebuild)
    )
    return cast(dict, cast(MaterializeResult, result).metadata)


def raw_asset_outputs() -> list[Output]:
    """Run ``raw_s3_logs`` for ``DAY`` and return what it produced."""
    outputs = raw_s3_logs(build_asset_context(partition_key=DAY))
    return list(cast(Iterator[Output], outputs))


def run_raw_asset() -> pd.DataFrame:
    """The data ``raw_s3_logs`` produces for ``DAY``."""
    (output,) = raw_asset_outputs()
    return cast(pd.DataFrame, output.value)


def artifact_blob(client):
    """The stored artifact for ``DAY``, or ``None`` if there isn't one."""
    bucket, path = compacted_location(DAY)
    return client.bucket(bucket).get_blob(path)


def stored_artifact(client) -> tuple[bytes, dict]:
    """The decompressed contents and metadata of ``DAY``'s artifact."""
    blob = artifact_blob(client)
    assert blob is not None, "no artifact was stored"
    return zstd.decompress(blob.download_as_bytes()), blob.metadata


@pytest.mark.parametrize(
    ("environment", "base_path", "bucket", "prefix"),
    [
        pytest.param("prod", None, "metrics.catalyst.coop", "", id="prod_default"),
        pytest.param(
            "prod", "gs://other/prefix/", "other", "prefix/", id="prod_overridden"
        ),
        pytest.param(
            "local", "gs://other/prefix", "other", "prefix/", id="local_overridden"
        ),
    ],
)
def test_compacted_location(monkeypatch, environment, base_path, bucket, prefix):
    """Artifacts live under the output location, so overriding it redirects them."""
    monkeypatch.setenv("METRICS_PROD_ENV", environment)
    if base_path is None:
        monkeypatch.delenv("PUDL_METRICS_GCS_BASE_PATH", raising=False)
    else:
        monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", base_path)
    path = f"{prefix}raw/pudl_s3_logs/{DAY}.log.zst"
    assert compacted_location(DAY) == (bucket, path)


@pytest.mark.parametrize(
    "base_path",
    [None, "gs://metrics.catalyst.coop/scratch"],
    ids=["default", "inside_the_bucket"],
)
def test_only_prod_runs_may_use_the_production_bucket(monkeypatch, base_path):
    """A development or test run can't read or replace production artifacts."""
    monkeypatch.delenv("METRICS_PROD_ENV", raising=False)
    if base_path is None:
        monkeypatch.delenv("PUDL_METRICS_GCS_BASE_PATH", raising=False)
    else:
        monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", base_path)
    with pytest.raises(ValueError):
        compacted_location(DAY)


def test_compact_then_read(fake_s3_gcs, s3_fixture_blobs):
    """A day is composed and stored as one artifact, which ``raw_s3_logs`` reads."""
    blobs = s3_fixture_blobs("normal_v1")
    client = fake_s3_gcs(blobs)

    metadata = run_compacted_asset()

    assert metadata["action"] == "built"
    assert metadata["source_object_count"] == len(blobs)
    assert client.bucket(BUCKET).compose_calls  # reduced server-side
    records, stored_metadata = stored_artifact(client)
    assert records == b"".join(blobs.values())
    assert stored_metadata["source_object_count"] == str(len(blobs))
    assert run_raw_asset().shape == (records.count(b"\n"), N_COLUMNS_V1)


def test_compacted_reuses_the_artifact_unless_rebuilding(fake_s3_gcs):
    """An existing artifact is kept, without even listing the raw logs."""
    client = fake_s3_gcs({f"{DAY}-00-00-00-a": RECORD_A})
    run_compacted_asset()
    source = client.bucket(BUCKET)
    source.blob(f"{DAY}-00-00-01-b").upload_from_string(RECORD_B)
    source.list_prefixes.clear()

    assert run_compacted_asset()["action"] == "reused"
    assert source.list_prefixes == []
    assert stored_artifact(client)[0] == RECORD_A

    assert run_compacted_asset(rebuild=True)["action"] == "built"
    records, metadata = stored_artifact(client)
    assert records == RECORD_A + RECORD_B
    assert metadata["source_object_count"] == "2"


def test_compacted_refuses_partition_not_yet_transferred(fake_s3_gcs, monkeypatch):
    """A build before the day's logs are fully in GCS is refused, not partial."""
    client = fake_s3_gcs({f"{DAY}-00-00-00-a": RECORD_A})
    monkeypatch.setattr(
        s3_module, "_utcnow", lambda: datetime(2024, 6, 16, 1, 0, tzinfo=UTC)
    )
    with pytest.raises(RuntimeError):
        run_compacted_asset()
    assert artifact_blob(client) is None


def test_empty_day_is_stored_as_an_empty_artifact(fake_s3_gcs):
    """The empty artifact marks the day as compacted; "no-data" is what the run reports."""
    client = fake_s3_gcs({})

    assert run_compacted_asset()["action"] == "no-data"

    records, metadata = stored_artifact(client)
    assert records == b""
    assert metadata["source_object_count"] == "0"
    assert raw_asset_outputs() == []


def test_compacted_falls_back_when_records_are_fused(fake_s3_gcs):
    """A source object missing its newline fuses records under compose; the
    build detects that and redoes the day with newline padding."""
    client = fake_s3_gcs(
        {
            f"{DAY}-00-00-00-a": RECORD_A.rstrip(b"\n"),  # no trailing newline
            f"{DAY}-00-00-01-b": RECORD_B,
        }
    )
    metadata = run_compacted_asset()
    assert stored_artifact(client)[0] == RECORD_A + RECORD_B
    assert metadata["source_object_count"] == 2


def test_raw_s3_logs_missing_artifact_is_an_error(fake_s3_gcs):
    """A partition that was never compacted must not silently look empty."""
    fake_s3_gcs({})
    with pytest.raises(FileNotFoundError):
        run_raw_asset()
