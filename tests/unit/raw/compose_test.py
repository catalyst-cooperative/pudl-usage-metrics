"""Tests for server-side compose reduction."""

from typing import Any, cast

import pytest

from usage_metrics.raw import compose
from usage_metrics.raw.compose import compose_day

BUCKET = "b"
DAY = "2024-06-15"


def _bucket(make_client, n: int, prefix=DAY) -> tuple:
    blobs = {f"{prefix}-{i:06d}": f"record {i}\n".encode() for i in range(n)}
    bucket = make_client({BUCKET: blobs}).bucket(BUCKET)
    return bucket, b"".join(blobs[k] for k in sorted(blobs))


def _joined(blobs) -> bytes:
    return b"".join(b._data for b in blobs)


def _compose(bucket):
    return compose_day(bucket, DAY, DAY, workers=4)


@pytest.mark.parametrize(
    ("n", "n_blobs"),
    [(0, 0), (1, 1), (2, 1), (32, 1), (33, 1), (1024, 1), (2500, 3)],
)
def test_compose_day_preserves_bytes_in_name_order(make_client, n, n_blobs):
    """Two 32-way rounds leave ceil(n / 1024) blobs that concatenate to the sources.

    The fake enforces the 32-source and 1024-component limits, and a lone object is
    passed through without any compose call.
    """
    bucket, expected = _bucket(make_client, n)
    blobs, count = _compose(bucket)
    assert count == n
    assert len(blobs) == n_blobs
    assert all(cast(Any, b).components <= 1024 for b in blobs)
    assert _joined(blobs) == expected
    assert bool(bucket.compose_calls) == (n > 1)


def test_compose_day_only_reads_the_partition_prefix(make_client):
    """Objects from other days are neither listed as sources nor composed."""
    bucket, _ = _bucket(make_client, 40)
    other = bucket.blob("2024-06-16-000000")
    other._data = b"other\n"
    other._store()
    blobs, count = _compose(bucket)
    assert count == 40
    assert b"other" not in _joined(blobs)


def test_compose_day_intermediates_are_scratch_and_unique_per_run(make_client):
    """Intermediates live under the tmp prefix and a rerun never collides."""
    bucket, expected = _bucket(make_client, 100)
    _compose(bucket)
    first = list(bucket.compose_calls)
    blobs, _ = _compose(bucket)
    assert _joined(blobs) == expected
    assert all(n.startswith(f"{compose.COMPOSE_TMP_PREFIX}/{DAY}/") for n in first)
    assert len(set(bucket.compose_calls)) == len(bucket.compose_calls)


def test_compose_requires_the_destination_not_to_exist(make_client, monkeypatch):
    """Every compose is made with ``if_generation_match=0`` so it's retry-safe."""
    bucket, _ = _bucket(make_client, 100)
    seen = []
    blob_type = type(bucket.blob("x"))
    real = blob_type.compose

    def spy(self, sources, if_generation_match=None, **kwargs):
        seen.append(if_generation_match)
        return real(self, sources, if_generation_match=if_generation_match, **kwargs)

    monkeypatch.setattr(blob_type, "compose", spy)
    _compose(bucket)
    assert set(seen) == {0}
