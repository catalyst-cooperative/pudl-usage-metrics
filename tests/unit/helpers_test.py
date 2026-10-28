"""Unit tests for the retrying and geocoding helpers."""

import logging

import ipinfo
import ipinfo.error
import pandas as pd
import pytest
import requests

from usage_metrics import helpers


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Record backoff sleeps instead of waiting."""
    recorded = []
    monkeypatch.setattr(helpers.time, "sleep", recorded.append)
    return recorded


def _flaky(*errors, result="ok"):
    """Build a function that raises each of ``errors`` in turn, then returns."""
    calls = []

    def func():
        calls.append(1)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]
        return result

    return func, calls


def test_retry_request_backs_off_exponentially(sleeps):
    """Retries wait delay, delay * backoff, ... and then succeed."""
    func, calls = _flaky(*[requests.exceptions.Timeout()] * 3)
    assert helpers.retry_request(retries=5, delay=1, backoff=2)(func)() == "ok"
    assert len(calls) == 4
    assert sleeps == [1, 2, 4]


def test_retry_request_delay_resets_between_calls(sleeps):
    """The backoff of one call doesn't carry over to the next call."""
    decorated = helpers.retry_request(retries=3, delay=1, backoff=2)
    for _ in range(2):
        func, _ = _flaky(requests.exceptions.Timeout())
        decorated(func)()
    assert sleeps == [1, 1]


def test_retry_request_gives_up(sleeps):
    """After the last attempt the failure is raised, chained to its cause."""
    func, calls = _flaky(*[requests.exceptions.Timeout()] * 5)
    with pytest.raises(RuntimeError, match="Max retries") as excinfo:
        helpers.retry_request(retries=3, delay=1)(func)()
    assert len(calls) == 3
    assert isinstance(excinfo.value.__cause__, requests.exceptions.Timeout)


def test_retry_request_ignores_other_exceptions():
    """Exceptions that aren't in retry_on are raised on the first attempt."""
    func, calls = _flaky(ValueError("nope"))
    with pytest.raises(ValueError):
        helpers.retry_request()(func)()
    assert len(calls) == 1


def test_geocode_ip_retries_5xx(tmp_path, monkeypatch, sleeps):
    """A 503 from ipinfo is retried and the eventual success is cached."""
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(tmp_path))
    func, calls = _flaky(ipinfo.error.APIError(503, {"error": "timeout"}))
    monkeypatch.setattr(helpers, "_geocode_ip", lambda ip: func() and {"ip": ip})
    assert helpers.geocode_ip("1.2.3.4") == {"ip": "1.2.3.4"}
    assert helpers.geocode_ip("1.2.3.4") == {"ip": "1.2.3.4"}
    assert len(calls) == 2  # One failure, one success; the second lookup was cached.
    assert sleeps == [1]


def test_geocode_ip_does_not_retry_client_errors(tmp_path, monkeypatch, sleeps):
    """A 4xx (e.g. a bad token) fails immediately and isn't cached."""
    monkeypatch.setenv("PUDL_METRICS_LOCAL_DATA_DIR", str(tmp_path))
    func, calls = _flaky(*[ipinfo.error.APIError(403, {"error": "bad token"})] * 2)
    monkeypatch.setattr(helpers, "_geocode_ip", lambda ip: func() and {"ip": ip})
    with pytest.raises(ipinfo.error.APIError):
        helpers.geocode_ip("1.2.3.4")
    assert len(calls) == 1
    assert sleeps == []
    with pytest.raises(ipinfo.error.APIError):
        helpers.geocode_ip("1.2.3.4")  # Not cached, so it calls the API again.
    assert len(calls) == 2


def test_geocode_op_retry_policy_shape():
    """Guard the op-level policy: exponential, jittered backoff."""
    policy = helpers.GEOCODE_OP_RETRY_POLICY
    assert policy.max_retries == 5
    assert policy.delay == 30
    assert policy.backoff == helpers.Backoff.EXPONENTIAL
    assert policy.jitter == helpers.Jitter.PLUS_MINUS


def test_geocode_ips_logs_counts_and_timing(monkeypatch, caplog):
    """The op reports how many unique ips it geocoded and how long that took."""
    monkeypatch.setattr(helpers, "geocode_ip", lambda ip: {"ip": ip})
    df = pd.DataFrame({"remote_ip": ["1.1.1.1", "1.1.1.1", "2.2.2.2", None]})
    with caplog.at_level(logging.INFO):
        out = helpers.geocode_ips(df)
    assert len(out) == 4
    assert "Geocoding 2 unique ips from 4 log rows" in caplog.text
    assert "Geocoded 2 unique ips in" in caplog.text
