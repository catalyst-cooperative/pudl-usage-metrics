"""Fixtures for the integration tests."""

import os
from uuid import uuid4

import pytest

LIVE_TEST_PATH = "gs://test.catalyst.coop/live-tests"
"""Where tests that need real GCS write. A lifecycle rule on the bucket deletes
objects after 7 days."""


@pytest.fixture
def live_gcs_path() -> str:
    """A new folder in the test bucket, for a test that needs real GCS.

    The test is skipped unless ``PUDL_METRICS_LIVE_TESTS=1``. Running it takes Google
    Cloud credentials that can write to the bucket.
    """
    if os.getenv("PUDL_METRICS_LIVE_TESTS") != "1":
        pytest.skip("Set PUDL_METRICS_LIVE_TESTS=1 to run tests against real GCS.")
    return f"{LIVE_TEST_PATH}/{uuid4().hex}"
