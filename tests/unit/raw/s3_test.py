"""Tests for the S3 log extractor."""

import os
from collections.abc import Iterator
from compression import zstd
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from typing import cast

import pandas as pd
import polars as pl
import pytest
from dagster import (
    Definitions,
    Failure,
    MaterializeResult,
    Output,
    build_asset_context,
    define_asset_job,
    load_assets_from_modules,
)

from usage_metrics import helpers
from usage_metrics.checks import build_schema_checks
from usage_metrics.core import s3 as core_s3
from usage_metrics.out import s3 as out_s3
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
from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager

BUCKET = "pudl-s3-logs.catalyst.coop"
DAY = "2024-06-15"
NEXT_DAY = "2024-06-16"

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
    agent: str = "curl/8.0",
    *,
    ip: str = "198.51.100.1",
    requester: str = "-",
    day: str = DAY,
    request_id: str = "RID",
) -> str:
    """One S3 access-log record with ``N_COLUMNS_V1`` columns, made on ``day``."""
    made = date.fromisoformat(day).strftime("%d/%b/%Y")
    return (
        f"owner bkt [{made}:00:00:00 +0000] {ip} {requester} {request_id} "
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
def fake_s3_gcs(fake_gcs, patch_download_many, download_dir, mocker):
    """Factory: a fake client (source + artifact buckets) wired into the assets."""

    mocker.patch.dict(os.environ, {"PUDL_METRICS_GCS_BASE_PATH": "gs://test-outputs"})

    def _make(source_blobs: dict[str, bytes]):
        client = fake_gcs({BUCKET: source_blobs, compacted_location(DAY)[0]: {}})
        mocker.patch.object(
            s3_module, "S3Extractor", side_effect=lambda: S3Extractor(client=client)
        )
        return client

    return _make


def compacted_asset_results(
    rebuild: bool = False, day: str = DAY
) -> list[MaterializeResult]:
    """Run ``compacted_s3_logs`` for ``day`` and return what it produced."""
    results = compacted_s3_logs(
        build_asset_context(partition_key=day), CompactedS3LogsConfig(rebuild=rebuild)
    )
    return list(cast(Iterator[MaterializeResult], results))


def run_compacted_asset(rebuild: bool = False, day: str = DAY) -> dict:
    """The metadata of what ``compacted_s3_logs`` produces for ``day``."""
    (result,) = compacted_asset_results(rebuild, day)
    return cast(dict, result.metadata)


def raw_asset_outputs(day: str = DAY) -> list[Output]:
    """Run ``raw_s3_logs`` for ``day`` and return what it produced."""
    outputs = raw_s3_logs(build_asset_context(partition_key=day))
    return list(cast(Iterator[Output], outputs))


def run_raw_asset(day: str = DAY) -> pd.DataFrame:
    """The data ``raw_s3_logs`` produces for ``day``."""
    (output,) = raw_asset_outputs(day)
    return cast(pd.DataFrame, output.value)


def artifact_blob(client, day: str = DAY):
    """The stored artifact for ``day``, or ``None`` if there isn't one."""
    bucket, path = compacted_location(day)
    return client.bucket(bucket).get_blob(path)


def stored_artifact(client, day: str = DAY) -> tuple[bytes, dict]:
    """The decompressed contents and metadata of ``day``'s artifact."""
    blob = artifact_blob(client, day)
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
def test_compacted_location(mocker, environment, base_path, bucket, prefix):
    """Artifacts live under the output location, so overriding it redirects them."""
    # patch.dict restores the whole environment, including what's changed after it.
    mocker.patch.dict(os.environ, {"METRICS_PROD_ENV": environment})
    if base_path is None:
        os.environ.pop("PUDL_METRICS_GCS_BASE_PATH", None)
    else:
        os.environ["PUDL_METRICS_GCS_BASE_PATH"] = base_path
    path = f"{prefix}raw/pudl_s3_logs/{DAY}.log.zst"
    assert compacted_location(DAY) == (bucket, path)


@pytest.mark.parametrize(
    "base_path",
    [None, "gs://metrics.catalyst.coop/scratch"],
    ids=["default", "inside_the_bucket"],
)
def test_only_prod_runs_may_use_the_production_bucket(mocker, base_path):
    """A development or test run can't read or replace production artifacts."""
    mocker.patch.dict(os.environ)
    os.environ.pop("METRICS_PROD_ENV", None)
    if base_path is None:
        os.environ.pop("PUDL_METRICS_GCS_BASE_PATH", None)
    else:
        os.environ["PUDL_METRICS_GCS_BASE_PATH"] = base_path
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
    previous_day = records.count(b"[14/Jun/2024:")
    assert len(run_raw_asset()) == records.count(b"\n") - previous_day


def test_compacted_reuses_the_artifact_unless_rebuilding(fake_s3_gcs):
    """An existing artifact is kept, without even listing the raw logs."""
    client = fake_s3_gcs({f"{DAY}-00-00-00-a": RECORD_A})
    run_compacted_asset()
    source = client.bucket(BUCKET)
    source.blob(f"{DAY}-00-00-01-b").upload_from_string(RECORD_B)
    source.list_prefixes.clear()

    assert run_compacted_asset()["action"] == "reused"
    assert DAY not in source.list_prefixes
    assert stored_artifact(client)[0] == RECORD_A

    assert run_compacted_asset(rebuild=True)["action"] == "built"
    records, metadata = stored_artifact(client)
    assert records == RECORD_A + RECORD_B
    assert metadata["source_object_count"] == "2"


def test_compacted_waits_until_the_next_days_logs_are_due(fake_s3_gcs, mocker):
    """A day published early would be missing requests, so nothing is built, and
    running early isn't a failure.
    """
    client = fake_s3_gcs({f"{DAY}-00-00-00-a": RECORD_A})
    mocker.patch.object(
        s3_module,
        "_utcnow",
        autospec=True,
        return_value=datetime(2024, 6, 17, 1, 0, tzinfo=UTC),
    )
    assert compacted_asset_results() == []
    assert artifact_blob(client) is None


@pytest.mark.parametrize(
    ("day", "next_day"),
    [
        pytest.param(DAY, NEXT_DAY, id="same_layout"),
        # aws_region first appears in the logs of the second day.
        pytest.param("2026-02-15", "2026-02-16", id="new_column_added"),
    ],
)
def test_every_record_is_published_by_exactly_one_day(fake_s3_gcs, day, next_day):
    """Requests go to the day they were made, wherever they were logged, and the rare
    record logged much later goes with the day it was logged.
    """
    previous = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    long_ago = (date.fromisoformat(day) - timedelta(days=9)).isoformat()

    def logs(*records: tuple[str, str], logged: str) -> bytes:
        extra = " us-west-2" if logged > "2026-02-15" else ""
        return "".join(
            log_line(request_id=request_id, day=made).replace("\n", f"{extra}\n")
            for request_id, made in records
        ).encode()

    fake_s3_gcs(
        {
            f"{day}-00-00-00-a": logs(
                ("previous", previous),
                ("own", day),
                ("straggler", long_ago),
                logged=day,
            ),
            f"{next_day}-00-00-00-a": logs(
                ("late", day), ("tomorrow", next_day), logged=next_day
            ),
        }
    )
    run_compacted_asset(day=day)

    published = run_raw_asset(day=day)

    assert sorted(published["request_id"]) == ["late", "own", "straggler"]


def test_a_day_is_published_when_the_next_has_no_logs(fake_s3_gcs):
    """One day's missing logs mustn't hold back the day before it."""
    fake_s3_gcs({f"{DAY}-00-00-00-a": RECORD_A})

    assert run_compacted_asset()["next_day_action"] == "no logs"
    assert len(run_raw_asset()) == 1


@pytest.mark.parametrize(
    ("remaining", "outcome"),
    [
        pytest.param({}, nullcontext(), id="no_raw_logs"),
        pytest.param(
            {f"{DAY}-00-00-00-a": RECORD_A},
            pytest.raises(Failure),
            id="fewer_raw_logs",
        ),
        pytest.param(
            {f"{DAY}-00-00-00-a": b"", f"{DAY}-00-00-01-b": b""},
            pytest.raises(Failure),
            id="no_records",
        ),
    ],
)
def test_rebuild_never_replaces_the_artifact_with_less(fake_s3_gcs, remaining, outcome):
    """With no raw logs left the artifact is still published; with only some, or
    nothing in them, the run fails.
    """
    client = fake_s3_gcs({f"{DAY}-00-00-00-a": RECORD_A, f"{DAY}-00-00-01-b": RECORD_B})
    run_compacted_asset()
    source = client.bucket(BUCKET)
    source._blobs.clear()
    for name, data in remaining.items():
        source.blob(name).upload_from_string(data)

    with outcome:
        run_compacted_asset(rebuild=True)
    assert stored_artifact(client)[0] == RECORD_A + RECORD_B


@pytest.mark.parametrize(
    ("rebuild", "other_count", "outcome"),
    [
        pytest.param(False, 1, nullcontext(), id="build"),
        pytest.param(True, 1, nullcontext(), id="rebuild"),
        pytest.param(False, 2, pytest.raises(Failure), id="different_count"),
    ],
)
def test_a_log_stored_by_another_run_is_not_overwritten(
    fake_s3_gcs, mocker, rebuild, other_count, outcome
):
    """Runs for neighbouring days can build the same compacted log at once. That's
    only a problem if they found different numbers of raw logs.
    """
    client = fake_s3_gcs({f"{DAY}-00-00-00-a": RECORD_A})
    if rebuild:
        run_compacted_asset()
    bucket, path = compacted_location(DAY)
    build = s3_module._build_artifact

    def build_while_another_run_stores(context, ext, day):
        built = build(context, ext, day)
        if day == DAY:
            other = client.bucket(bucket).blob(path)
            other.metadata = {"source_object_count": str(other_count)}
            other.upload_from_string(zstd.compress(RECORD_B))
        return built

    mocker.patch.object(
        s3_module,
        "_build_artifact",
        autospec=True,
        side_effect=build_while_another_run_stores,
    )

    with outcome:
        assert run_compacted_asset(rebuild=rebuild)["action"] == "reused"
    assert stored_artifact(client)[0] == RECORD_B


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


@pytest.mark.parametrize(
    "logs",
    [
        pytest.param(
            {"a": log_line().replace("REST.GET.OBJECT", "S3.TRANSITION_INT.OBJECT")},
            id="only_lifecycle_transitions",
        ),
        pytest.param(
            {"a": log_line().replace("REST.GET.OBJECT", "REST.PUT.OBJECT")},
            id="nothing_downloaded",
        ),
    ],
)
def test_s3_job_succeeds_on_a_day_with_nothing_to_summarize(
    fake_s3_gcs, mocker, tmp_path, logs
):
    """A quiet day mustn't fail forever."""
    mocker.patch.object(
        helpers, "geocode_ip", autospec=True, side_effect=lambda ip: {"ip": ip}
    )
    fake_s3_gcs(
        {
            f"{DAY}-00-00-00-{name}": line.replace("owner", "a" * 64, 1).encode()
            for name, line in logs.items()
        }
    )
    assets = load_assets_from_modules([s3_module, core_s3, out_s3])
    job = Definitions(
        assets=assets,
        asset_checks=build_schema_checks(assets),
        jobs=[define_asset_job("job")],
        resources={
            "parquet_manager": PartitionedParquetIOManager(
                base_path=str(tmp_path / "parquet")
            )
        },
    ).resolve_job_def("job")

    assert job.execute_in_process(partition_key=DAY, raise_on_error=False).success
