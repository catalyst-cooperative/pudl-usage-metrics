"""Tests for the S3 log extractor."""

from compression import zstd

import polars as pl
import pytest
from dagster import materialize

from usage_metrics.raw.s3 import (
    FusedRecordsError,
    S3Extractor,
    _has_embedded_quotes,
    raw_s3_logs,
    zstd_with_guard,
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
    ("subdir", "n_columns"),
    [("normal_v1", 27), ("normal_v2", 28)],
)
def test_load_file_column_counts(write_log, s3_fixture_blobs, subdir, n_columns):
    """Each schema era parses to the expected column count."""
    path = write_log(b"".join(s3_fixture_blobs(subdir).values()))
    assert S3Extractor().load_file(path).width == n_columns


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


def test_load_file_ragged_partition_forces_28_columns(write_log, s3_fixture_blobs):
    """On 2026-02-25 a mixed-width file is coerced to 28 columns."""
    ext = S3Extractor()
    ext.partition_key = "2026-02-25"
    path = write_log(b"".join(s3_fixture_blobs("ragged").values()))
    assert ext.load_file(path).width == 28


def test_load_file_ragged_other_partition_raises_with_note(tmp_path, s3_fixture_blobs):
    """A ragged file outside 2026-02-25 raises, annotated with the path."""
    ext = S3Extractor()
    ext.partition_key = "2025-01-01"
    combined = tmp_path / "combined"
    combined.write_bytes(b"".join(s3_fixture_blobs("ragged").values()))
    with pytest.raises(pl.exceptions.ComputeError) as excinfo:
        ext.load_file(combined)
    assert any("Extraction failed" in note for note in excinfo.value.__notes__)


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


def test_load_file_drops_line_with_embedded_json_user_agent(write_log, monkeypatch):
    """A file with one line whose User-Agent embeds raw JSON parses, minus that line.

    pip's ``pip/24.3.1 {"ci":null,...}`` User-Agent has unescaped double quotes inside
    a quoted field, which breaks CSV tokenization. The line is dropped rather than
    guessed at.
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
    # 1 of 2 lines is far over the real limit, which is exercised below.
    monkeypatch.setattr("usage_metrics.raw.s3.MAX_DROPPED_LINE_FRACTION", 0.9)
    df = S3Extractor().load_file(write_log((bad_line + good_line).encode()))
    assert df.width == 27
    assert df.height == 1
    assert df.row(0)[4] == "198.51.100.2"
    assert df.row(0)[17] == "Mozilla/5.0 (X11; Linux)"


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


# --- raw_s3_logs asset ---------------------------------------------------


@pytest.mark.parametrize(
    ("subdir", "n_columns"),
    [("normal_v1", 27), (None, None)],
    ids=["with_logs", "empty_partition"],
)
def test_raw_s3_logs_asset_materializes(
    s3_fixture_blobs,
    make_client,
    patch_download_many,
    download_dir,
    monkeypatch,
    subdir,
    n_columns,
):
    """End to end: fake bucket -> raw_s3_logs -> one pandas frame (or an empty one)."""
    blobs = s3_fixture_blobs(subdir) if subdir else {}
    client = make_client({S3Extractor().bucket_name: blobs})
    monkeypatch.setattr(
        "usage_metrics.raw.s3.S3Extractor",
        lambda *a, **k: S3Extractor(client=client),
    )
    result = materialize([raw_s3_logs], partition_key="2024-06-15")
    assert result.success
    df = result.output_for_node("raw_s3_logs")
    if not blobs:
        assert df.empty
        return
    assert len(df) == sum(v.count(b"\n") for v in blobs.values())
    assert df.shape[1] == n_columns


def _bad_and_good_lines():
    """A full-width S3 log line with a pip-style User-Agent, and a clean one."""
    template = (
        "owner bkt [19/Sep/2026:00:00:00 +0000] 198.51.100.1 - RID REST.GET.OBJECT k "
        '"GET /k HTTP/1.1" 200 - 100 200 5 4 "-" {agent} '
        "- hid SigV4 ECDHE AuthHeader host TLSv1.2 - -\n"
    )
    return (
        template.format(
            agent='"pip/24.3.1 {"ci":null,"openssl_version":"OpenSSL 3.0.2"}"'
        ),
        template.format(agent='"curl/8.0"'),
    )


def test_load_file_fails_when_too_many_lines_dropped(tmp_path):
    """Dropping more than MAX_DROPPED_LINE_FRACTION of lines fails the partition."""
    bad, good = _bad_and_good_lines()
    path = tmp_path / "f"
    path.write_text(bad * 5 + good * 995)  # 0.5%
    with pytest.raises(ValueError, match="investigate"):
        S3Extractor().load_file(path)


def test_load_file_tolerates_a_few_dropped_lines(tmp_path):
    """A dropped fraction under the limit is kept out of the data, not fatal."""
    bad, good = _bad_and_good_lines()
    path = tmp_path / "f"
    path.write_text(bad + good * 9999)  # 0.01%
    assert S3Extractor().load_file(path).height == 9999
