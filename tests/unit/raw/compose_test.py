"""Tests for server-side compose reduction."""

from typing import Any, cast

import pytest
from google.api_core.exceptions import PreconditionFailed

from usage_metrics.raw import compose
from usage_metrics.raw.compose import compose_day

BUCKET = "b"


def _bucket(make_client, n: int, prefix="2024-06-15") -> tuple:
    blobs = {f"{prefix}-{i:06d}": f"record {i}\n".encode() for i in range(n)}
    bucket = make_client({BUCKET: blobs}).bucket(BUCKET)
    return bucket, b"".join(blobs[k] for k in sorted(blobs))


def _joined(blobs) -> bytes:
    return b"".join(b._data for b in blobs)


@pytest.mark.parametrize("n", [2, 32, 33, 100, 1024, 2500])
def test_compose_day_preserves_bytes_in_name_order(make_client, n):
    """Whatever the size, the reduced blobs concatenate to the sources in order."""
    bucket, expected = _bucket(make_client, n)
    blobs, count = compose_day(bucket, "2024-06-15", "2024-06-15", workers=4)
    assert count == n
    assert _joined(blobs) == expected


def test_compose_day_reduces_to_about_n_over_1024(make_client):
    """Two 32-way rounds leave ceil(n / 1024) objects; the fake enforces the
    32-source and 1024-component limits."""
    bucket, _ = _bucket(make_client, 2500)
    blobs, _ = compose_day(bucket, "2024-06-15", "2024-06-15", workers=4)
    assert len(blobs) == 3
    assert all(cast(Any, b).components <= 1024 for b in blobs)


def test_compose_day_empty_prefix(make_client):
    """No matching objects: nothing to compose."""
    bucket, _ = _bucket(make_client, 3, prefix="2024-06-16")
    assert compose_day(bucket, "2024-06-15", "2024-06-15", workers=2) == ([], 0)
    assert bucket.compose_calls == []


def test_compose_day_single_object_is_passed_through(make_client):
    """One object needs no compose at all."""
    bucket, expected = _bucket(make_client, 1)
    blobs, count = compose_day(bucket, "2024-06-15", "2024-06-15", workers=2)
    assert count == 1
    assert _joined(blobs) == expected
    assert bucket.compose_calls == []


def test_compose_day_only_reads_the_partition_prefix(make_client):
    """Objects from other days are neither listed as sources nor composed."""
    bucket, _ = _bucket(make_client, 40)
    other = bucket.blob("2024-06-16-000000")
    other._data = b"other\n"
    other._store()
    blobs, count = compose_day(bucket, "2024-06-15", "2024-06-15", workers=2)
    assert count == 40
    assert b"other" not in _joined(blobs)


def test_compose_day_intermediates_are_scratch_and_unique_per_run(make_client):
    """Intermediates live under the tmp prefix and a rerun never collides."""
    bucket, expected = _bucket(make_client, 100)
    compose_day(bucket, "2024-06-15", "2024-06-15", workers=2)
    first = list(bucket.compose_calls)
    blobs, _ = compose_day(bucket, "2024-06-15", "2024-06-15", workers=2)
    assert _joined(blobs) == expected
    assert all(n.startswith(f"{compose.COMPOSE_TMP_PREFIX}/2024-06-15/") for n in first)
    assert len(set(bucket.compose_calls)) == len(bucket.compose_calls)


def test_compose_requires_the_destination_not_to_exist(make_client, monkeypatch):
    """Every compose is made with ``if_generation_match=0`` so it's retry-safe."""
    bucket, _ = _bucket(make_client, 100)
    seen = []
    real = type(bucket.blob("x")).compose

    def spy(self, sources, if_generation_match=None, **kwargs):
        seen.append(if_generation_match)
        return real(self, sources, if_generation_match=if_generation_match, **kwargs)

    monkeypatch.setattr(type(bucket.blob("x")), "compose", spy)
    compose_day(bucket, "2024-06-15", "2024-06-15", workers=2)
    assert seen
    assert set(seen) == {0}


def test_fake_bucket_rejects_overwriting_a_compose_destination(make_client):
    """Sanity check on the fake: a repeated conditional create is a 412."""
    bucket, _ = _bucket(make_client, 3)
    sources = bucket.list_blobs(prefix="2024-06-15")
    bucket.blob("dest").compose(sources, if_generation_match=0)
    with pytest.raises(PreconditionFailed):
        bucket.blob("dest").compose(sources, if_generation_match=0)
