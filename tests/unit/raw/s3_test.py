"""Tests for the S3 log extractor."""

from compression import zstd
from datetime import UTC, datetime
from typing import cast

import pandas as pd
import polars as pl
import pytest
from dagster import MaterializeResult, build_asset_context

from usage_metrics.raw import extract
from usage_metrics.raw import s3 as s3_module
from usage_metrics.raw.s3 import (
    CompactedS3LogsConfig,
    FusedRecordsError,
    S3Extractor,
    _drop_lines_with_embedded_quotes,
    compacted_location,
    compacted_s3_logs,
    raw_s3_logs,
    zstd_with_guard,
)

BUCKET = "pudl-s3-logs.catalyst.coop"
DAY = "2024-06-15"


# --- get_blob_prefix / filter_blobs ----------------------------------------


def test_get_blob_prefix_is_partition_date(partition_context):
    """The listing prefix is the partition's ISO date."""
    ext = S3Extractor()
    assert ext.get_blob_prefix(partition_context("2024-06-15")) == "2024-06-15"


def test_filter_blobs_keeps_only_matching_date(make_client, partition_context):
    """filter_blobs keeps blobs whose name starts with the partition date."""
    blobs = {
        "2024-06-15-00-00-00-a": b"",
        "2024-06-16-00-00-00-b": b"",
        "2024-06-15": b"",
    }
    client = make_client({BUCKET: blobs})
    ext = S3Extractor(client=client)
    listed = client.bucket(BUCKET).list_blobs()
    kept = {b.name for b in ext.filter_blobs(partition_context("2024-06-15"), listed)}
    assert kept == {"2024-06-15-00-00-00-a", "2024-06-15"}


# --- compose / download worker counts ------------------------------------------


def test_compose_and_download_use_their_own_worker_counts(
    make_client, partition_context, download_dir, monkeypatch
):
    """compose() calls run on compose_workers threads; downloads on download_workers."""
    seen = {}
    monkeypatch.setattr(
        s3_module,
        "compose_day",
        lambda bucket, prefix, key, workers: (
            seen.setdefault("compose", workers) and ([], 0)
        ),
    )
    monkeypatch.setattr(
        extract.transfer_manager,
        "download_many",
        lambda pairs, **kwargs: seen.setdefault("download", kwargs["max_workers"]),
    )
    client = make_client({BUCKET: {}})
    ext = S3Extractor(client=client, compose_workers=7, download_workers=3)
    ext.download_gcs_blobs(partition_context(DAY), download_dir)
    assert seen == {"compose": 7, "download": 3}


# --- load_file -----------------------------------------------------------


@pytest.mark.parametrize(
    ("subdir", "n_columns"),
    [("normal_v1", 27), ("normal_v2", 28)],
)
def test_load_file_column_counts(tmp_path, s3_fixture_blobs, subdir, n_columns):
    """Each schema era parses to the expected column count."""
    ext = S3Extractor()
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs(subdir).values()))
    df = ext.load_file(combined)
    assert df.width == n_columns


def test_load_file_preserves_quoted_and_dash_fields(tmp_path):
    """Quoted fields with spaces stay intact; '-' is read literally."""
    line = (
        "owner bkt [15/Jun/2024:00:00:00 +0000] 198.51.100.1 - RID REST.GET.OBJECT k "
        '"GET /k HTTP/1.1" 200 - 100 200 5 4 "-" "Mozilla/5.0 (X11; Linux)" - hid '
        "SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )
    path = tmp_path / "f"
    path.write_text(line)
    row = S3Extractor().load_file(path).row(0)
    assert row[9] == "GET /k HTTP/1.1"
    assert row[17] == "Mozilla/5.0 (X11; Linux)"
    assert row[5] == "-"


def test_load_file_ragged_partition_forces_28_columns(tmp_path, s3_fixture_blobs):
    """On 2026-02-25 a mixed-width file is coerced to 28 columns."""
    ext = S3Extractor()
    ext.partition_key = "2026-02-25"
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs("ragged").values()))
    df = ext.load_file(combined)
    assert df.width == 28


def test_load_file_ragged_other_partition_raises_with_note(tmp_path, s3_fixture_blobs):
    """A ragged file outside 2026-02-25 raises, annotated with the path."""
    ext = S3Extractor()
    ext.partition_key = "2025-01-01"
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs("ragged").values()))
    with pytest.raises(pl.exceptions.ComputeError) as excinfo:
        ext.load_file(combined)
    assert any("Extraction failed" in note for note in excinfo.value.__notes__)


# --- _drop_lines_with_embedded_quotes ---------------------------------------


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        pytest.param(
            '"GET /k HTTP/1.1" 200 "-" "Mozilla/5.0 (X11; Linux)" - hid\n',
            '"GET /k HTTP/1.1" 200 "-" "Mozilla/5.0 (X11; Linux)" - hid\n',
            id="well_formed_fields_are_kept",
        ),
        pytest.param(
            '"GET /k HTTP/1.1" 200 "-" "pip/24.3.1 {"ci":null,"cpu":"x86_64"}" - hid\n',
            "",
            id="pip_style_embedded_json_in_user_agent_is_dropped",
        ),
        pytest.param(
            '"GET /k HTTP/1.1" 200 "https://x.com/?q="weird"" "Mozilla/5.0" - hid\n',
            "",
            id="embedded_quote_in_referer_not_just_user_agent_is_dropped",
        ),
        pytest.param(
            '"GET /k HTTP/1.1" 200 "-" "" - hid\n',
            '"GET /k HTTP/1.1" 200 "-" "" - hid\n',
            id="empty_quoted_field_is_kept",
        ),
        pytest.param(
            "- - - - -\n",
            "- - - - -\n",
            id="no_quoted_fields_at_all_is_kept",
        ),
    ],
)
def test_drop_lines_with_embedded_quotes_single_line(line, expected):
    """A line with a malformed field is dropped whole; a clean line is kept."""
    assert _drop_lines_with_embedded_quotes(line) == expected


def test_drop_lines_with_embedded_quotes_keeps_good_lines_around_a_bad_one():
    """Only the malformed line is dropped; good lines before and after survive."""
    good_a = '"GET /a HTTP/1.1" 200 "-" "Mozilla/5.0" - hid\n'
    bad = '"GET /b HTTP/1.1" 200 "-" "pip/1.0 {"a":1}" - hid\n'
    good_b = '"GET /c HTTP/1.1" 200 "-" "curl/8.0" - hid\n'
    assert _drop_lines_with_embedded_quotes(good_a + bad + good_b) == good_a + good_b


def test_load_file_drops_line_with_embedded_json_user_agent(tmp_path):
    """End to end: a file with one malformed line still parses, minus that line.

    Regression test: pip's User-Agent embeds raw JSON, e.g.
    ``pip/24.3.1 {"ci":null,...,"openssl_version":"OpenSSL 3.0.2"}``, with
    literal unescaped double quotes -- and here, a space inside a JSON string
    value -- inside a field AWS already quotes. That combination breaks CSV
    tokenization ("not properly escaped") and used to fail the whole
    partition (hit during the 2026-09 backfill, e.g. 2026-09-19). Rather than
    guess at repairing inherently ambiguous content, the malformed line is
    dropped and the rest of the day's data survives -- the same approach
    widely-used S3-log parsers take (their quoted-field pattern just fails to
    match these lines).
    """
    bad_line = (
        "owner bkt [19/Sep/2026:00:00:00 +0000] 198.51.100.1 - RID REST.GET.OBJECT k "
        '"GET /k HTTP/1.1" 200 - 100 200 5 4 "-" '
        '"pip/24.3.1 {"ci":null,"openssl_version":"OpenSSL 3.0.2"}" '
        "- hid SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )
    good_line = (
        "owner bkt [19/Sep/2026:00:00:01 +0000] 198.51.100.2 - RID2 REST.GET.OBJECT k2 "
        '"GET /k2 HTTP/1.1" 200 - 100 200 5 4 "-" "Mozilla/5.0 (X11; Linux)" '
        "- hid2 SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )
    path = tmp_path / "f"
    path.write_text(bad_line + good_line)
    df = S3Extractor().load_file(path)
    assert df.width == 27
    assert df.height == 1
    assert df.row(0)[4] == "198.51.100.2"
    assert df.row(0)[17] == "Mozilla/5.0 (X11; Linux)"


def test_load_file_empty_raises_no_data_error(tmp_path):
    """An empty file raises NoDataError for extract() to handle."""
    path = tmp_path / "empty"
    path.write_bytes(b"")
    with pytest.raises(pl.exceptions.NoDataError):
        S3Extractor().load_file(path)


# --- compressed input to load_file -----------------------------------------------


def _compressed(tmp_path, data: bytes):
    path = tmp_path / "day.log.zst"
    path.write_bytes(zstd.compress(data))
    return path


@pytest.mark.parametrize(
    ("subdir", "n_columns"),
    [("normal_v1", 27), ("normal_v2", 28)],
)
def test_load_file_reads_zstd_same_as_plain(
    tmp_path, s3_fixture_blobs, subdir, n_columns
):
    """A zstd-compressed combined file parses identically to the plain one."""
    data = b"".join(s3_fixture_blobs(subdir).values())
    plain = tmp_path / "plain"
    plain.write_bytes(data)
    ext = S3Extractor()
    expected = ext.load_file(plain)
    actual = ext.load_file(_compressed(tmp_path, data))
    assert actual.width == n_columns
    assert actual.equals(expected)


def test_load_file_ragged_partition_zstd_forces_28_columns(tmp_path, s3_fixture_blobs):
    """The 2026-02-25 ragged-day workaround also works on compressed input."""
    ext = S3Extractor()
    ext.partition_key = "2026-02-25"
    path = _compressed(tmp_path, b"".join(s3_fixture_blobs("ragged").values()))
    assert ext.load_file(path).width == 28


def test_load_file_zstd_drops_line_with_embedded_quotes(tmp_path):
    """The embedded-quote fallback decompresses ``.zst`` input before cleaning."""
    bad_line = (
        "owner bkt [19/Sep/2026:00:00:00 +0000] 198.51.100.1 - RID REST.GET.OBJECT k "
        '"GET /k HTTP/1.1" 200 - 100 200 5 4 "-" '
        '"pip/24.3.1 {"ci":null,"openssl_version":"OpenSSL 3.0.2"}" '
        "- hid SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )
    good_line = (
        "owner bkt [19/Sep/2026:00:00:01 +0000] 198.51.100.2 - RID2 REST.GET.OBJECT k2 "
        '"GET /k2 HTTP/1.1" 200 - 100 200 5 4 "-" "Mozilla/5.0 (X11; Linux)" '
        "- hid2 SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )
    df = S3Extractor().load_file(_compressed(tmp_path, (bad_line + good_line).encode()))
    assert df.height == 1
    assert df.row(0)[4] == "198.51.100.2"


# --- zstd_with_guard -----------------------------------------------------------

REC_A = b"owner bkt [15/Jun/2024:00:00:00 +0000] 1.1.1.1 owner RID1 op k\n"
REC_B = b"owner bkt [15/Jun/2024:00:00:01 +0000] 2.2.2.2 - RID2 op k\n"


def test_zstd_with_guard_round_trips(tmp_path):
    """Well-formed records are compressed unchanged, and the line count returned."""
    src = tmp_path / "src"
    src.write_bytes(REC_A + REC_B)
    dest = tmp_path / "dest.zst"
    assert zstd_with_guard(src, dest) == 2
    assert zstd.decompress(dest.read_bytes()) == REC_A + REC_B


def test_zstd_with_guard_allows_owner_id_elsewhere_in_a_record(tmp_path):
    """The bucket owner can legitimately be the requester (REC_A) -- not a fusion."""
    src = tmp_path / "src"
    src.write_bytes(REC_A)
    assert zstd_with_guard(src, tmp_path / "dest.zst") == 1


def test_zstd_with_guard_detects_fused_records(tmp_path):
    """Two records on one line (a missing newline before compose) are rejected."""
    src = tmp_path / "src"
    src.write_bytes(REC_A + REC_B.rstrip(b"\n") + REC_A + REC_B)
    with pytest.raises(FusedRecordsError, match="Line 2"):
        zstd_with_guard(src, tmp_path / "dest.zst")


def test_zstd_with_guard_can_be_disabled(tmp_path):
    """``guard=False`` skips the check for input that is already newline-padded."""
    src = tmp_path / "src"
    src.write_bytes(b"whatever\nlines\n")
    assert zstd_with_guard(src, tmp_path / "dest.zst", guard=False) == 2


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


@pytest.mark.parametrize("subdir", ["normal_v1", "normal_v2"])
def test_raw_s3_logs_reads_the_artifact(s3_client, s3_fixture_blobs, tmp_path, subdir):
    """raw_s3_logs returns the same frame as parsing the day's plain files."""
    blobs = s3_fixture_blobs(subdir)
    client = s3_client({})
    data = b"".join(blobs.values())
    _put_artifact(client, data, len(blobs))
    df = _raw()
    plain = tmp_path / "plain"
    plain.write_bytes(data)
    pd.testing.assert_frame_equal(df, S3Extractor().load_file(plain).to_pandas())
    assert len(df) == data.count(b"\n")


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


def test_raw_s3_logs_ragged_day_from_artifact(s3_client, s3_fixture_blobs):
    """The 2026-02-25 mixed-width day parses to 28 columns from the artifact."""
    client = s3_client({})
    blobs = s3_fixture_blobs("ragged")
    _put_artifact(client, b"".join(blobs.values()), len(blobs), key="2026-02-25")
    df = _raw("2026-02-25")
    assert df.shape[1] == 28


def test_compact_then_read_end_to_end(s3_client, s3_fixture_blobs):
    """compacted_s3_logs followed by raw_s3_logs reproduces the day's rows."""
    blobs = s3_fixture_blobs("normal_v1")
    s3_client(blobs)
    _build()
    df = _raw()
    assert len(df) == sum(v.count(b"\n") for v in blobs.values())
    assert df.shape[1] == 27


def test_compacted_location_follows_output_path(monkeypatch):
    """The compacted artifact is under the configured output location."""
    monkeypatch.delenv("PUDL_METRICS_GCS_BASE_PATH", raising=False)
    assert compacted_location("2024-01-01") == (
        "metrics.catalyst.coop",
        "raw/pudl_s3_logs/2024-01-01.log.zst",
    )
    monkeypatch.setenv("PUDL_METRICS_GCS_BASE_PATH", "gs://test-bucket/trial")
    assert compacted_location("2024-01-01") == (
        "test-bucket",
        "trial/raw/pudl_s3_logs/2024-01-01.log.zst",
    )
