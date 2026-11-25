"""Tests for the S3 log extractor."""

from compression import zstd
from datetime import UTC, datetime
from typing import cast

import pandas as pd
import polars as pl
import pytest
from dagster import MaterializeResult, build_asset_context

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
"""Columns before February 2026: 26 fields, one more since the timestamp splits in two."""
N_COLUMNS_V2 = 28
"""Columns since ``aws_region`` was added."""

PIP_AGENT = 'pip/24.3.1 {"ci":null,"openssl_version":"OpenSSL 3.0.2"}'
"""A User-Agent with unescaped quotes inside a quoted field."""


def log_line(agent: str = "curl/8.0", *, ip: str = "198.51.100.1") -> str:
    """One S3 access-log record with ``N_COLUMNS_V1`` columns."""
    return (
        f"owner bkt [19/Sep/2026:00:00:00 +0000] {ip} - RID REST.GET.OBJECT k "
        f'"GET /k HTTP/1.1" 200 - 100 200 5 4 "-" "{agent}" '
        "- hid SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )


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


@pytest.fixture(params=["plain", "zstd"])
def write_log(request, tmp_path):
    """Return a function that writes log bytes to a plain or zstd-compressed file."""

    def _write(data: bytes):
        if request.param == "zstd":
            path = tmp_path / "day.log.zst"
            path.write_bytes(zstd.compress(data))
        else:
            path = tmp_path / "day.log"
            path.write_bytes(data)
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
    write_log, s3_fixture_blobs, subdir, partition_key, n_columns
):
    """Every record is read, with the column count of its schema era."""
    data = b"".join(s3_fixture_blobs(subdir).values())
    ext = S3Extractor()
    ext.partition_key = partition_key
    df = ext.load_file(write_log(data))
    assert df.shape == (data.count(b"\n"), n_columns)


def test_load_file_ragged_other_partition_raises_with_note(tmp_path, s3_fixture_blobs):
    """A ragged file outside 2026-02-25 raises, annotated with the path."""
    ext = S3Extractor()
    ext.partition_key = "2025-01-01"
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs("ragged").values()))
    with pytest.raises(pl.exceptions.ComputeError) as excinfo:
        ext.load_file(combined)
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


def test_load_file_drops_lines_with_embedded_quotes(write_log):
    """A few lines with unescaped quotes are dropped, and the rest of the day parses.

    Regression test for the "not properly escaped" CSV error that failed whole
    partitions in the 2026-09 backfill (e.g. 2026-09-19). pip's User-Agent embeds raw
    JSON, including a space inside a string value, within a quoted field.
    """
    bad = log_line(PIP_AGENT, ip="198.51.100.9")
    path = write_log((bad + log_line() * 9999).encode())  # 0.01% bad
    df = S3Extractor().load_file(path)
    assert df.shape == (9999, N_COLUMNS_V1)
    assert df["column_4"].unique().to_list() == ["198.51.100.1"]


def test_load_file_fails_when_too_many_lines_dropped(write_log):
    """Dropping more than MAX_DROPPED_LINE_FRACTION of lines fails the partition."""
    path = write_log((log_line(PIP_AGENT) * 5 + log_line() * 995).encode())  # 0.5%
    with pytest.raises(ValueError):
        S3Extractor().load_file(path)


# --- zstd_with_guard -----------------------------------------------------------

REC_A = b"owner bkt [15/Jun/2024:00:00:00 +0000] 1.1.1.1 owner RID1 op k\n"
REC_B = b"owner bkt [15/Jun/2024:00:00:01 +0000] 2.2.2.2 - RID2 op k\n"


def test_zstd_with_guard_round_trips(tmp_path):
    """Well-formed records are compressed unchanged, and the line count returned.

    The bucket owner can appear in the body of a record (as it does in ``REC_A``)
    without failing the record-fusion check.
    """
    src = tmp_path / "src"
    src.write_bytes(REC_A + REC_B)
    dest = tmp_path / "dest.zst"
    assert zstd_with_guard(src, dest) == 2
    assert zstd.decompress(dest.read_bytes()) == REC_A + REC_B


def test_zstd_with_guard_detects_fused_records(tmp_path):
    """Two records on one line (a missing newline before compose) are rejected."""
    src = tmp_path / "src"
    src.write_bytes(REC_A + REC_B.rstrip(b"\n") + REC_A + REC_B)
    with pytest.raises(FusedRecordsError, match="Line 2"):
        zstd_with_guard(src, tmp_path / "dest.zst")


def test_zstd_with_guard_rejects_a_malformed_first_line(tmp_path):
    """A first line with no record prefix can't set the signature, so it's rejected."""
    src = tmp_path / "src"
    src.write_bytes(b"\n")
    with pytest.raises(FusedRecordsError, match="Not a log record"):
        zstd_with_guard(src, tmp_path / "dest.zst")


def test_zstd_with_guard_empty_file(tmp_path):
    """An empty file compresses to an empty stream with zero lines."""
    src = tmp_path / "src"
    src.write_bytes(b"")
    dest = tmp_path / "dest.zst"
    assert zstd_with_guard(src, dest) == 0
    assert zstd.decompress(dest.read_bytes()) == b""


# --- compacted_s3_logs / raw_s3_logs assets -------------------------------------


@pytest.fixture
def s3_client(make_client, patch_download_many, download_dir, monkeypatch):
    """Factory: a fake client (source + artifact buckets) wired into the assets."""

    def _make(
        source_blobs: dict[str, bytes], artifacts: dict[str, bytes] | None = None
    ):
        client = make_client(
            {BUCKET: source_blobs, compacted_location(DAY)[0]: artifacts or {}}
        )
        monkeypatch.setattr(
            s3_module, "S3Extractor", lambda *a, **k: S3Extractor(client=client)
        )
        return client

    return _make


def _artifact(client, key=DAY):
    bucket, path = compacted_location(key)
    return client.bucket(bucket).get_blob(path)


def _put_artifact(client, data: bytes, count: int, key=DAY):
    bucket, path = compacted_location(key)
    blob = client.bucket(bucket).blob(path)
    blob._data = zstd.compress(data)
    blob.metadata = {"source_object_count": str(count)}
    blob._store()


def _build(key=DAY, rebuild=False):
    result = compacted_s3_logs(
        build_asset_context(partition_key=key), CompactedS3LogsConfig(rebuild=rebuild)
    )
    return cast(dict, cast(MaterializeResult, result).metadata)


def _raw(key=DAY) -> pd.DataFrame:
    return cast(pd.DataFrame, raw_s3_logs(build_asset_context(partition_key=key)))


def test_compacted_builds_artifact(s3_client, s3_fixture_blobs):
    """No artifact yet: compose, download, compress, upload with metadata."""
    blobs = s3_fixture_blobs("normal_v1")
    client = s3_client(blobs)
    metadata = _build()
    artifact = _artifact(client)
    assert zstd.decompress(artifact._data) == b"".join(blobs.values())
    assert artifact.metadata["source_object_count"] == str(len(blobs))
    assert "built_at" in artifact.metadata
    assert artifact.content_type == "application/zstd"
    assert metadata["action"] == "built"
    assert metadata["source_object_count"] == len(blobs)
    assert client.bucket(BUCKET).compose_calls  # reduced server-side


def test_compacted_reuses_existing_artifact_without_touching_the_source(s3_client):
    """An existing artifact is reused with no listing or compose of the raw logs."""
    client = s3_client({f"{DAY}-00-00-00-a": REC_A}, {})
    _put_artifact(client, REC_A, 1)
    metadata = _build()
    assert metadata["action"] == "reused"
    assert client.bucket(BUCKET).list_prefixes == []
    assert client.bucket(BUCKET).compose_calls == []


def test_compacted_rebuild_ignores_existing_artifact(s3_client):
    """``rebuild=True`` rebuilds from the source even if an artifact exists."""
    client = s3_client({f"{DAY}-00-00-00-a": REC_A, f"{DAY}-00-00-01-b": REC_B})
    _put_artifact(client, REC_A, 1)
    metadata = _build(rebuild=True)
    assert metadata["action"] == "built"
    assert zstd.decompress(_artifact(client)._data) == REC_A + REC_B
    assert _artifact(client).metadata["source_object_count"] == "2"


def test_compacted_refuses_partition_not_yet_transferred(s3_client, monkeypatch):
    """A build before the day's logs are fully in GCS is refused, not partial."""
    client = s3_client({f"{DAY}-00-00-00-a": REC_A})
    monkeypatch.setattr(
        s3_module, "_utcnow", lambda: datetime(2024, 6, 16, 1, 0, tzinfo=UTC)
    )
    with pytest.raises(RuntimeError, match="may not be fully transferred"):
        _build()
    assert _artifact(client) is None


def test_compacted_empty_day_records_an_empty_artifact(s3_client):
    """No source objects: an empty artifact marks the day as checked."""
    client = s3_client({})
    metadata = _build()
    assert metadata["action"] == "no-data"
    assert _artifact(client).metadata["source_object_count"] == "0"
    assert zstd.decompress(_artifact(client)._data) == b""


def test_compacted_falls_back_when_records_are_fused(s3_client):
    """A source object missing its newline fuses records under compose; the
    build detects that and redoes the day with newline padding."""
    blobs = {
        f"{DAY}-00-00-00-a": REC_A.rstrip(b"\n"),  # no trailing newline
        f"{DAY}-00-00-01-b": REC_B,
    }
    client = s3_client(blobs)
    metadata = _build()
    assert zstd.decompress(_artifact(client)._data) == REC_A + REC_B
    assert metadata["source_object_count"] == 2


def test_raw_s3_logs_missing_artifact_is_an_error(s3_client):
    """A partition that was never compacted must not silently look empty."""
    s3_client({})
    with pytest.raises(FileNotFoundError, match="compacted_s3_logs"):
        _raw()


def test_raw_s3_logs_empty_day(s3_client):
    """An empty-day artifact yields an empty frame."""
    client = s3_client({})
    _put_artifact(client, b"", 0)
    assert _raw().empty


def test_compact_then_read_end_to_end(s3_client, s3_fixture_blobs):
    """compacted_s3_logs followed by raw_s3_logs reproduces the day's rows."""
    blobs = s3_fixture_blobs("normal_v1")
    s3_client(blobs)
    _build()
    df = _raw()
    assert len(df) == sum(v.count(b"\n") for v in blobs.values())
    assert df.shape[1] == 27
