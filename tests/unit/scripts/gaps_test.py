"""Tests for `usage-metrics gaps`."""

from datetime import date

from click.testing import CliRunner

from usage_metrics.scripts.cli import cli
from usage_metrics.scripts.gaps import METRICS_BUCKET, Source, find_gaps


class _FakeBlob:
    def __init__(self, name: str):
        self.name = name


class _FakeBucket:
    def __init__(self, names: list[str]):
        self._names = sorted(names)

    def list_blobs(self, prefix: str | None = None, max_results: int | None = None):
        hits = [
            _FakeBlob(n) for n in self._names if prefix is None or n.startswith(prefix)
        ]
        return hits[:max_results] if max_results else hits


class _FakeClient:
    def __init__(self, buckets: dict[str, list[str]]):
        self._buckets = buckets

    def bucket(self, name: str) -> _FakeBucket:
        return _FakeBucket(self._buckets.get(name, []))


SOURCE = Source("s3", "core_s3_logs", "raw-bucket", lambda d: d.isoformat())


def _client() -> _FakeClient:
    return _FakeClient(
        {
            METRICS_BUCKET: [
                "core_s3_logs/2026-09-01--2026-09-02.parquet",
                "core_s3_logs/2026-09-03--2026-09-04.parquet",
                "core_s3_logs/_temp/junk.txt",
            ],
            "raw-bucket": [
                "2026-09-01-00-00-00-AAAA",
                "2026-09-02-12-30-00-BBBB",
                "2026-09-03-00-00-00-CCCC",
                # nothing for 2026-09-04
            ],
        }
    )


def test_find_gaps_reports_missing_processed_that_has_raw():
    """09-02 is unprocessed and has raw data; 09-04 is unprocessed but has none."""
    gaps = find_gaps(SOURCE, _client(), date(2026, 9, 1), date(2026, 9, 4))
    assert gaps == [date(2026, 9, 2)]


def test_find_gaps_without_raw_check_reports_all_missing():
    gaps = find_gaps(
        SOURCE, _client(), date(2026, 9, 1), date(2026, 9, 4), check_raw=False
    )
    assert gaps == [date(2026, 9, 2), date(2026, 9, 4)]


def test_find_gaps_empty_when_everything_processed():
    gaps = find_gaps(SOURCE, _client(), date(2026, 9, 1), date(2026, 9, 1))
    assert gaps == []


def test_gaps_help_needs_no_credentials():
    """`usage-metrics gaps -h` must not construct a storage client."""
    result = CliRunner().invoke(cli, ["gaps", "-h"])
    assert result.exit_code == 0, result.output
    assert "raw data but no processed output" in result.output
