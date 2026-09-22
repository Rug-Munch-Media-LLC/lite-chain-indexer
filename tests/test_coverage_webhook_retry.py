"""Coverage tests for ``lite_indexer.webhooks.delivery.retry`` backoff helpers.

Hermetic — no network, no NATS. Complements ``tests/test_webhooks.py``
(which drives the full DLQ lifecycle) by pinning the pure backoff
helpers directly: the Svix schedule shape, the ±20% jitter bounds, and
the jitter-disabled path.
"""

from __future__ import annotations

from lite_indexer.webhooks.delivery.helpers import _SVIX_RETRY_DELAYS
from lite_indexer.webhooks.delivery.retry import RetryMixin


class _JitterHost(RetryMixin):
    """Minimal host exposing only the state ``_jittered`` reads."""

    def __init__(self, *, jitter: bool) -> None:
        self._jitter = jitter


def test_svix_schedule_starts_immediate_then_grows() -> None:
    assert _SVIX_RETRY_DELAYS[0] == 0.0
    assert _SVIX_RETRY_DELAYS[1] == 5.0
    assert _SVIX_RETRY_DELAYS[2] == 300.0
    assert _SVIX_RETRY_DELAYS[-1] == 36000.0


def test_svix_schedule_is_monotonic_non_decreasing() -> None:
    assert all(a <= b for a, b in zip(_SVIX_RETRY_DELAYS, _SVIX_RETRY_DELAYS[1:], strict=False))


def test_zero_delay_is_never_jittered() -> None:
    host = _JitterHost(jitter=True)
    assert host._jittered(0.0) == 0.0


def test_jitter_disabled_returns_delay_verbatim() -> None:
    host = _JitterHost(jitter=False)
    assert host._jittered(300.0) == 300.0


def test_jitter_stays_within_plus_minus_20_percent() -> None:
    host = _JitterHost(jitter=True)
    for _ in range(200):
        value = host._jittered(100.0)
        assert 80.0 <= value <= 120.0


def test_jitter_actually_varies() -> None:
    host = _JitterHost(jitter=True)
    values = {host._jittered(100.0) for _ in range(50)}
    assert len(values) > 1


def test_negative_delay_is_returned_verbatim() -> None:
    host = _JitterHost(jitter=True)
    assert host._jittered(-5.0) == -5.0
