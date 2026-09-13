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

import contextlib
from typing import Any

try:
    from prometheus_client import REGISTRY, Counter, Gauge, ProcessCollector

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

# Gauges report a current level (not a running total). Unlabeled gauges live
# here; labeled alert-contract gauges are declared in _LABELED_GAUGE_SPECS.
_GAUGE_NAMES: tuple[str, ...] = (
    "rpc_consensus_effective_rate",
    # Explicit process memory gauges (cgroup charge is what the kernel OOMs
    # on; process_resident_memory_bytes alone is a proxy).
    "lite_indexer_rss_bytes",
    "lite_indexer_cgroup_memory_bytes",
)


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
    """Minimal labeled ``Gauge`` stand-in when prometheus_client is missing."""

    __slots__ = ("_key", "_values")

    def __init__(self, values: dict[tuple[tuple[str, str], ...], float] | None = None) -> None:
        self._values: dict[tuple[tuple[str, str], ...], float] = {} if values is None else values
        self._key: tuple[tuple[str, str], ...] = ()

    def labels(self, **labels: str) -> _PlainGauge:
        child = _PlainGauge(self._values)
        child._key = tuple(sorted(labels.items()))
        return child

    def set(self, value: float) -> None:
        """Set the gauge to an absolute ``value``."""
        self._values[self._key] = float(value)

    def get(self) -> float:
        """Return the current gauge value."""
        return self._values.get(self._key, 0.0)


def _make_counter(name: str, labelnames: tuple[str, ...] = ()) -> Any:
    """Create a Prometheus Counter or plain fallback for a counter name.

    Prometheus counters are registered with the default registry so a
    standard ``/metrics`` endpoint (or ``generate_latest()``) picks them
    up for scraping.
    """
    if PROMETHEUS_AVAILABLE:
        return Counter(name, f"LiteChainIndexer {name} counter", labelnames=labelnames)
    return _PlainCounter()


def _make_gauge(name: str, labelnames: tuple[str, ...] = ()) -> Any:
    """Create a Prometheus Gauge (or plain fallback) on the default registry."""
    if PROMETHEUS_AVAILABLE:
        return Gauge(name, f"LiteChainIndexer {name} gauge", labelnames=labelnames)
    return _PlainGauge()


# Counter name → counter object. Populated eagerly for the known names so
# that a stray typo surfaces early; unknown names are created lazily.
_COUNTERS: dict[str, Any] = {name: _make_counter(name) for name in _COUNTER_NAMES}
# Gauge name → gauge object (eager, same typo-surfacing intent).
_GAUGES: dict[str, Any] = {name: _make_gauge(name) for name in _GAUGE_NAMES}

# ── Alert-contract metrics ─────────────────────────────────────────────
# fleet-infra/prometheus/rules/lite-indexer.yml references these exact names.
# They are created eagerly so the /metrics endpoint always exposes the series
# (zero-valued until populated) and the alert rules have something to scrape.
#
# Labeled gauges: {metric name: label names}
_LABELED_GAUGE_SPECS: dict[str, tuple[str, ...]] = {
    # Per-chain latest persisted block (BootstrapNeeded alert + dashboards).
    "lite_indexer_state_last_block": ("chain",),
    "lite_indexer_last_block": ("chain",),
    # Healthy RPC endpoints per chain (RpcPoolExhausted alert).
    "lite_indexer_rpc_pool_healthy_endpoints": ("chain",),
    # Reconciliation divergence (DataReconciliationMismatch alert).
    "lite_indexer_reconciliation_discrepancy_pct": ("chain",),
    # NATS publisher circuit state (NatsCircuitOpen alert).
    "lite_indexer_nats_circuit_open": ("chain",),
    # Per-provider quota consumption (degrade-ladder observability).
    "lite_indexer_quota_consumed_pct": ("provider",),
}
_LABELED_GAUGES: dict[str, Any] = {name: _make_gauge(name, labels) for name, labels in _LABELED_GAUGE_SPECS.items()}

# Alert-facing counters: {metric name: label names}. Prometheus appends
# ``_total`` to the exposed sample name, so ``lite_indexer_rpc_failures`` is
# scraped as ``lite_indexer_rpc_failures_total``.
_ALERT_COUNTER_SPECS: dict[str, tuple[str, ...]] = {
    "lite_indexer_rpc_failures": ("chain",),
    "lite_indexer_state_regression_detected": ("chain",),
    "lite_indexer_reorg_unknown": ("chain",),
    "lite_indexer_parquet_export_failed": ("chain", "date"),
    "lite_indexer_webhook_delivery_failed": (),
}
_ALERT_COUNTERS: dict[str, Any] = {name: _make_counter(name, labels) for name, labels in _ALERT_COUNTER_SPECS.items()}

# Existing counters that back an alert-facing metric: incrementing the source
# also moves the alert series (keeps the runtime code paths untouched).
_UNLABELED_ALIASES: dict[str, str] = {
    "webhook_dlq_enqueued": "lite_indexer_webhook_delivery_failed",
}

# Sentinel label value used to materialise a zero series for every labeled
# alert metric at import, so the /metrics endpoint always exposes the family
# before the first real observation. A zero series never trips an alert
# (rate/increase of 0), and label-selector alerts ignore "none".
_NO_LABEL_VALUE = "none"


def _prime_labeled_metrics() -> None:
    """Materialise zero-valued sentinel series for labeled alert metrics.

    prometheus_client omits a labeled Counter family from ``collect()`` until
    a child exists, so without this the alert's metric name would be absent
    from ``/metrics`` until the first failure — exactly when the alert should
    already be able to see "no failures yet".
    """
    for name, labels in _LABELED_GAUGE_SPECS.items():
        _LABELED_GAUGES[name].labels(**{label: _NO_LABEL_VALUE for label in labels}).set(0.0)
    for name, labels in _ALERT_COUNTER_SPECS.items():
        counter = _ALERT_COUNTERS[name]
        if labels:
            counter.labels(**{label: _NO_LABEL_VALUE for label in labels}).inc(0.0)
        else:
            counter.inc(0.0)


_prime_labeled_metrics()


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
    alias = _UNLABELED_ALIASES.get(name)
    if alias is not None:
        alert = _ALERT_COUNTERS.get(alias)
        if alert is None:
            alert = _make_counter(alias)
            _ALERT_COUNTERS[alias] = alert
        alert.inc(amount)


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


def set_labeled_gauge(name: str, value: float, **labels: str) -> None:
    """Set a labeled alert-facing gauge (created lazily if unknown).

    Args:
        name: Gauge name (e.g. ``lite_indexer_state_last_block``).
        value: Absolute value to store.
        **labels: Label values (e.g. ``chain="ethereum"``).
    """
    gauge_obj = _LABELED_GAUGES.get(name)
    if gauge_obj is None:
        gauge_obj = _make_gauge(name, tuple(labels))
        _LABELED_GAUGES[name] = gauge_obj
    if labels:
        gauge_obj.labels(**labels).set(float(value))
    else:
        gauge_obj.set(float(value))


def incr_labeled(name: str, amount: float = 1.0, **labels: str) -> None:
    """Increment an alert-facing labeled counter (created lazily if unknown).

    Args:
        name: Counter name (e.g. ``lite_indexer_rpc_failures``).
        amount: Amount to increment by (default: 1.0).
        **labels: Label values (e.g. ``chain="ethereum"``).
    """
    counter = _ALERT_COUNTERS.get(name)
    if counter is None:
        counter = _make_counter(name, tuple(labels))
        _ALERT_COUNTERS[name] = counter
    if labels:
        counter.labels(**labels).inc(amount)
    else:
        counter.inc(amount)


def ensure_process_collectors() -> None:
    """Register the Prometheus process collectors when not already present.

    ``prometheus_client`` auto-registers ``ProcessCollector`` /
    ``PlatformCollector`` on import for non-Windows platforms, which is what
    supplies ``process_resident_memory_bytes`` and
    ``process_cpu_seconds_total``. This is a guarded no-op there and an
    explicit registration elsewhere.
    """
    if not PROMETHEUS_AVAILABLE:
        return
    names = {family.name for family in REGISTRY.collect()}
    if "process_resident_memory_bytes" not in names:
        with contextlib.suppress(ValueError):  # already registered (race)
            REGISTRY.register(ProcessCollector())


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
    for labeled_gauge in _LABELED_GAUGES.values():
        clear = getattr(labeled_gauge, "clear", None)
        if callable(clear):
            clear()
        elif PROMETHEUS_AVAILABLE:
            labeled_gauge._value.set(0.0)  # noqa: SLF001
        else:
            labeled_gauge.set(0.0)


def get_metrics() -> dict[str, float]:
    """Backward-compatible alias for :func:`snapshot`.

    Replaces the per-module ``get_metrics()`` helpers that previously
    read/reset the scattered ``_metrics`` dicts.
    """
    return snapshot()


__all__ = [
    "incr",
    "incr_labeled",
    "get",
    "set_gauge",
    "set_labeled_gauge",
    "gauge",
    "snapshot",
    "reset",
    "get_metrics",
    "ensure_process_collectors",
    "PROMETHEUS_AVAILABLE",
]
