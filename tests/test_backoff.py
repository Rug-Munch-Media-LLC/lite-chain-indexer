"""Tests for jittered exponential backoff (rpc/backoff.py).

Covers:

* full jitter is within ``[0, upper]`` (and exactly ``upper`` when disabled)
* exponential ceiling ``min(cap, base*2**attempt)``
* seedable RNG (reproducible sequences)
* Retry-After is honoured EXACTLY (no jitter) in the get_logs 429 path
* jittered fallback when no Retry-After is advertised

Hermetic — no network, no disk.
"""

from __future__ import annotations

from unittest.mock import patch

import random
from typing import Any

import pytest

from lite_indexer.rpc import backoff as bo
from lite_indexer.rpc.backoff import exponential_backoff, full_jitter, set_jitter_seed


class TestFullJitter:
    def test_within_bounds(self):
        rng = random.Random(0)
        for _ in range(200):
            d = full_jitter(1.5, rng=rng)
            assert 0.0 <= d <= 1.5

    def test_non_positive_upper_is_zero(self):
        assert full_jitter(0.0) == 0.0
        assert full_jitter(-3.0) == 0.0

    def test_disabled_returns_upper(self):
        with patch.object(bo, "RPC_BACKOFF_JITTER", False):
            assert full_jitter(2.0) == 2.0

    def test_seedable_reproducible(self):
        set_jitter_seed(1234)
        a = [full_jitter(10.0) for _ in range(5)]
        set_jitter_seed(1234)
        b = [full_jitter(10.0) for _ in range(5)]
        assert a == b
        set_jitter_seed(None)


class TestExponentialBackoff:
    def test_ceiling_capped(self):
        patch.object(bo, "RPC_BACKOFF_JITTER", False).start()
        assert exponential_backoff(0, base=0.1, cap=1.0) == pytest.approx(0.1)
        assert exponential_backoff(1, base=0.1, cap=1.0) == pytest.approx(0.2)
        assert exponential_backoff(5, base=0.1, cap=1.0) == pytest.approx(1.0)  # capped

    def test_jitter_never_exceeds_ceiling(self):
        rng = random.Random(7)
        for attempt in range(6):
            d = exponential_backoff(attempt, base=0.1, cap=1.0, rng=rng)
            assert 0.0 <= d <= min(1.0, 0.1 * 2**attempt)


class _FakeResp:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = "rate limited"
        self.content = b"rate limited"

    def json(self):
        return {}

    def raise_for_status(self) -> None:
        return None


class _FakeHttp:
    def __init__(self, resp: _FakeResp) -> None:
        self._resp = resp

    async def post(self, url: str, **kw: Any):
        return self._resp


async def _sleeps_for_getlogs(headers: dict[str, str]) -> list[float]:
    import lite_indexer.rpc.client as rc

    sleeps: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    async def _noop(*a: Any, **k: Any) -> None:
        return None

    http = _FakeHttp(_FakeResp(429, headers))
    patch.object(rc.asyncio, "sleep", _fake_sleep).start()
    patch.object(rc, "apply_provider_rate_limit", _noop).start()
    patch.object(rc, "provider_throttle_before", _noop).start()
    patch.object(rc, "record_provider_response", lambda *a, **k: None).start()
    patch.object(rc, "_safe_record", lambda *a, **k: None).start()
    patch("lite_indexer.rpc.throttle.throttle_before", _noop).start()

    client = rc.RpcClient()

    async def _get_client() -> _FakeHttp:
        return http

    client._get_client = _get_client  # noqa: SLF001 — test seam
    await client.get_logs("https://rate.example", 0, 10, "0x" + "ab" * 32)
    return sleeps


@pytest.mark.asyncio
async def test_retry_after_is_exact():
    sleeps = await _sleeps_for_getlogs({"Retry-After": "7"})
    assert sleeps, "expected the 429 path to sleep"
    assert sleeps[0] == pytest.approx(7.0, abs=0.05)


@pytest.mark.asyncio
async def test_no_retry_after_uses_jitter():
    from lite_indexer.config import RATE_LIMIT_WAIT_CAP

    set_jitter_seed(99)
    sleeps = await _sleeps_for_getlogs({})
    set_jitter_seed(None)
    assert sleeps, "expected the 429 path to sleep"
    assert 0.0 <= sleeps[0] <= float(RATE_LIMIT_WAIT_CAP)
