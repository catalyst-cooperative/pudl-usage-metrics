"""Tests for server-side compose reduction.

These run ``compose_day`` against the fake in ``conftest.py``, so they check what we
ask GCS to do, not how GCS behaves.
"""

from usage_metrics.raw.compose import compose_day

BUCKET = "b"
DAY = "2024-06-15"
SOURCE_PREFIX = f"logs/{DAY}"


def test_compose_day_returns_exactly_the_days_bytes(fake_gcs):
    """All of the day's objects, once each and in name order, even on a rerun."""
    # 1025 objects need both compose rounds, and leave a partial group.
    day = {f"{SOURCE_PREFIX}-{i:06d}": f"record {i}\n".encode() for i in range(1025)}
    other_day = {"logs/2024-06-16-000000": b"other day\n"}
    bucket = fake_gcs({BUCKET: day | other_day}).bucket(BUCKET)

    for _ in range(2):
        outputs, count = compose_day(bucket, SOURCE_PREFIX, DAY, workers=4)

        assert count == len(day)
        composed = b"".join(blob.download_as_bytes() for blob in outputs)
        assert composed == b"".join(day.values())
