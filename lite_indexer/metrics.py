"""
Unified metrics registry for LiteChainIndexer.

Consolidates the per-module ``_metrics`` dicts that previously lived in
``rpc/client``, ``rpc/pool``, ``rpc/ratelimit``, ``rpc/health``,
``storage/clickhouse`` and ``storage/disk`` into a single module.

Counters are exposed as Prometheus ``Counter`` metrics when
``prometheus_client`` is installed (a project dependency) and degrade
to plain in-process counters otherwise, so the package still imports
without it.

Usage::

    from lite_indexer import metrics

    metrics.incr("rpc_errors")
    metrics.snapshot()  # -> {"rpc_errors": 1.0, "rpc_rate_limits": 0.0, ...}
"""

from __future__ import annotations

from typing import Any

try:
    from prometheus_client import Counter, Gauge

    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover — dependency is guarded, not required
    PROMETHEUS_AVAILABLE = False

# Every counter name used across the codebase.
_COUNTER_NAMES: tuple[str, ...] = (
    "rpc_errors",
    "rpc_rate_limits",
    "rpc_calls",
    "rpc_bytes",
    "rpc_circuit_open",
    "rpc_archive_fail_closed",
    "rpc_archive_endpoints_low",
    "rpc_provider_backoffs",
    "rpc_spillover",
    "getlogs_range_splits",
    "rpc_batch_calls",
    "rpc_batch_fallback",
    "rpc_cache_hits",
    "rpc_cache_misses",
    "rpc_cache_evictions",
    "rpc_consensus_verified",
    "rpc_consensus_mismatch",
    "ws_head_connected",
    "ws_head_messages",
    "ws_head_reconnects",
    "ws_head_fallback",
    "endpoints_verified",
    "endpoints_failed_verify",
    "clickhouse_inserts",
    "clickhouse_errors",
    "bloom_cache_hits",
    "bloom_cache_misses",
    "bloom_prefilter_skips",
    "parquet_files_exported",
    "parquet_rows_exported",
    "transfers_written",
    "dex_swaps_written",
    "deployers_written",
    "internal_transfers_written",
    "traces_collected",
    "trace_errors",
    "pricing_refreshes",
    "pricing_upserts",
    "whale_backfills",
    "nats_publishes",
    "nats_publish_failures",
    "webhook_registrations",
    "webhook_deliveries",
    "webhook_delivery_failures",
    "backfill_enqueued",
    "backfill_completed",
    "backfill_failed",
    "backfill_memory_paused",
    "backfill_window_deferred",
    "attestations_appended",
)

# Gauges report a current level (not a running total). Kept tiny: only the
# adaptive consensus rate needs one today.
_GAUGE_NAMES: tuple[str, ...] = ("rpc_consensus_effective_rate",)


class _PlainCounter:
    """Minimal ``Counter`` stand-in when prometheus_client is missing."""

    __slots__ = ("_value",)

    def __init__(self) -> None:
        self._value = 0.0

    def inc(self, amount: float = 1.0) -> None:
        """Increment the counter by ``amount`` (default 1.0)."""
        self._value += amount

    def get(self) -> float:
        """Return the current counter value."""
        return self._value

    def set(self, value: float) -> None:
        """Set the counter to an absolute ``value``."""
        self._value = value


class _PlainGauge:
    """Minimal ``Gauge`` stand-in when prometheus_client is missing."""

    __slots__ = ("_value",)

    def __init__(self) -> None:
        self._value = 0.0

    def set(self, value: float) -> None:
        """Set the gauge to an absolute ``value``."""
        self._value = value

    def get(self) -> float:
        """Return the current gauge value."""
        return self._value


def _make_counter(name: str) -> Any:
    """Create a Prometheus Counter or plain fallback for a counter name.

    Prometheus counters are registered with the default registry so a
    standard ``/metrics`` endpoint (or ``generate_latest()``) picks them
    up for scraping.
    """
    if PROMETHEUS_AVAILABLE:
        return Counter(name, f"LiteChainIndexer {name} counter")
    return _PlainCounter()


def _make_gauge(name: str) -> Any:
    """Create a Prometheus Gauge or plain fallback for a gauge name."""
    if PROMETHEUS_AVAILABLE:
        return Gauge(name, f"LiteChainIndexer {name} gauge")
    return _PlainGauge()


# Counter name → counter object. Populated eagerly for the known names so
# that a stray typo surfaces early; unknown names are created lazily.
_COUNTERS: dict[str, Any] = {name: _make_counter(name) for name in _COUNTER_NAMES}
# Gauge name → gauge object (eager, same typo-surfacing intent).
_GAUGES: dict[str, Any] = {name: _make_gauge(name) for name in _GAUGE_NAMES}


def incr(name: str, amount: float = 1.0) -> None:
    """Increment a named counter, creating it lazily if unknown.

    Args:
        name: Counter name (e.g. 'rpc_errors').
        amount: Amount to increment by (default: 1.0).
    """
    counter = _COUNTERS.get(name)
    if counter is None:
        counter = _make_counter(name)
        _COUNTERS[name] = counter
    counter.inc(amount)


def get(name: str) -> float:
    """Return the current value of a named counter (0.0 if never incremented).

    Args:
        name: Counter name.

    Returns:
        Current counter value.
    """
    counter = _COUNTERS.get(name)
    if counter is None:
        return 0.0
    if PROMETHEUS_AVAILABLE:
        # prometheus_client.Counter stores the value in its internal
        # ValueClass; there is no public read accessor.
        return float(counter._value.get())  # noqa: SLF001
    return float(counter.get())


def set_gauge(name: str, value: float) -> None:
    """Set a named gauge, creating it lazily if unknown.

    Args:
        name: Gauge name (e.g. 'rpc_consensus_effective_rate').
        value: Absolute value to store.
    """
    gauge_obj = _GAUGES.get(name)
    if gauge_obj is None:
        gauge_obj = _make_gauge(name)
        _GAUGES[name] = gauge_obj
    gauge_obj.set(float(value))


def gauge(name: str) -> float:
    """Return the current value of a named gauge (0.0 if never set)."""
    gauge_obj = _GAUGES.get(name)
    if gauge_obj is None:
        return 0.0
    if PROMETHEUS_AVAILABLE:
        return float(gauge_obj._value.get())  # noqa: SLF001
    return float(gauge_obj.get())


def snapshot() -> dict[str, float]:
    """Return all counter + gauge values as a plain dict.

    Useful for tests and for a future /metrics endpoint that renders the
    Prometheus exposition format.
    """
    out = {name: get(name) for name in _COUNTERS}
    out.update({name: gauge(name) for name in _GAUGES})
    return out


def reset() -> None:
    """Reset all counters and gauges to zero."""
    for counter in _COUNTERS.values():
        if PROMETHEUS_AVAILABLE:
            counter._value.set(0.0)  # noqa: SLF001
        else:
            counter.set(0.0)
    for gauge_obj in _GAUGES.values():
        if PROMETHEUS_AVAILABLE:
            gauge_obj._value.set(0.0)  # noqa: SLF001
        else:
            gauge_obj.set(0.0)


def get_metrics() -> dict[str, float]:
    """Backward-compatible alias for :func:`snapshot`.

    Replaces the per-module ``get_metrics()`` helpers that previously
    read/reset the scattered ``_metrics`` dicts.
    """
    return snapshot()


__all__ = [
    "incr",
    "get",
    "snapshot",
    "reset",
    "get_metrics",
    "PROMETHEUS_AVAILABLE",
]
