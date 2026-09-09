"""Tests for the eel hole (PUDL Viewer) log extractor."""

import json

from dagster import build_asset_context

from usage_metrics.raw.eel_hole import EelHoleExtractor

BUCKET = "pudl-viewer-logs.catalyst.coop"
STDOUT = "run.googleapis.com/stdout"


def _ctx(partition_key: str):
    return build_asset_context(partition_key=partition_key)


def _ndjson(*records: dict) -> bytes:
    return ("\n".join(json.dumps(r) for r in records) + "\n").encode()


def test_get_blob_prefix_is_dated_stdout_path():
    """The prefix narrows the listing to one day's Cloud Run stdout logs."""
    ext = EelHoleExtractor()
    assert ext.get_blob_prefix(_ctx("2026-09-01")) == f"{STDOUT}/2026/09/01"


def test_filter_blobs_keeps_only_matching_day(make_client):
    """filter_blobs keeps blobs under the partition day's prefix."""
    blobs = {
        f"{STDOUT}/2026/09/01/00:00:00_00:59:59_S0.json": b"",
        f"{STDOUT}/2026/09/01/01:00:00_01:59:59_S0.json": b"",
        f"{STDOUT}/2026/09/02/00:00:00_00:59:59_S0.json": b"",
    }
    client = make_client({BUCKET: blobs})
    ext = EelHoleExtractor(client=client)
    listed = client.bucket(BUCKET).list_blobs()
    kept = {blob.name for blob in ext.filter_blobs(_ctx("2026-09-01"), listed)}
    assert kept == {
        f"{STDOUT}/2026/09/01/00:00:00_00:59:59_S0.json",
        f"{STDOUT}/2026/09/01/01:00:00_01:59:59_S0.json",
    }


def test_load_file_reads_concatenated_ndjson(tmp_path):
    """A combined newline-delimited JSON file parses row-per-line."""
    combined = tmp_path / "combined"
    combined.write_bytes(
        _ndjson(
            {"insertId": "a", "jsonPayload": {"event": "preview"}},
            {"insertId": "b", "jsonPayload": {"event": "search"}},
        )
    )
    df = EelHoleExtractor().load_file(combined)
    assert list(df["insertId"]) == ["a", "b"]


def test_extract_combines_multiple_files(download_dir, run_extract):
    """extract() concatenates the day's files (concatenable_files=True)."""
    assert EelHoleExtractor.concatenable_files is True
    a = download_dir / "00_stdout"
    a.write_bytes(_ndjson({"insertId": "a"}))
    b = download_dir / "01_stdout"
    b.write_bytes(_ndjson({"insertId": "b"}).rstrip(b"\n"))  # no trailing newline
    df = run_extract(EelHoleExtractor(), _ctx("2026-09-01"), [a, b])
    assert sorted(df["insertId"]) == ["a", "b"]
