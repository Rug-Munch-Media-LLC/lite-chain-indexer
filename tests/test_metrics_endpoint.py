"""Tests for the Prometheus exporter and the alert-contract metrics.

Covers: the daemon HTTP exporter (start/stop/idempotency/busy-port no-op),
the alert-referenced metric names being present in the default registry, the
labeled/unlabeled gauge setters, and the global gauge export helper.
"""

from __future__ import annotations

import socket
import urllib.request
from types import SimpleNamespace
from typing import Any

import pytest
from prometheus_client import REGISTRY

from lite_indexer import metrics, metrics_server
from lite_indexer.cli._metrics_loop import export_chain_metrics, export_global_metrics

# Every metric referenced by fleet-infra/prometheus/rules/lite-indexer.yml
# that this repo is responsible for exporting (process_* come from the
# prometheus_client process collectors registered by ensure_process_collectors).
ALERT_METRICS: tuple[str, ...] = (
    "lite_indexer_rpc_pool_healthy_endpoints",
    "lite_indexer_rpc_failures_total",
    "lite_indexer_nats_circuit_open",
    "lite_indexer_reconciliation_discrepancy_pct",
    "lite_indexer_state_regression_detected_total",
    "lite_indexer_parquet_export_failed_total",
    "lite_indexer_webhook_delivery_failed_total",
    "lite_indexer_state_last_block",
    "lite_indexer_last_block",
    "lite_indexer_quota_consumed_pct",
    "lite_indexer_rss_bytes",
    "lite_indexer_cgroup_memory_bytes",
    "process_resident_memory_bytes",
    "process_cpu_seconds_total",
)


def _registry_names() -> set[str]:
    """All metric family + sample names currently registered."""
    names: set[str] = set()
    for family in REGISTRY.collect():
        names.add(family.name)
        names.update(sample.name for sample in family.samples)
    return names


@pytest.fixture(autouse=True)
def _stop_server_after_test() -> Any:
    """Never leak a metrics HTTP server between tests."""
    yield
    metrics_server.stop_metrics_server()


# ═══════════════════════════════════════════════════════════════════════
# Registry coverage
# ═══════════════════════════════════════════════════════════════════════


def test_alert_referenced_metrics_are_registered() -> None:
    metrics.ensure_process_collectors()
    missing = [name for name in ALERT_METRICS if name not in _registry_names()]
    assert missing == []


def test_process_collectors_available() -> None:
    metrics.ensure_process_collectors()
    names = _registry_names()
    assert "process_resident_memory_bytes" in names
    assert "process_cpu_seconds_total" in names


# ═══════════════════════════════════════════════════════════════════════
# Gauge / counter setters
# ═══════════════════════════════════════════════════════════════════════


def test_set_unlabeled_gauge() -> None:
    metrics.set_gauge("lite_indexer_rss_bytes", 12345678)
    assert REGISTRY.get_sample_value("lite_indexer_rss_bytes") == 12345678.0


def test_set_labeled_gauge() -> None:
    metrics.set_labeled_gauge("lite_indexer_state_last_block", 42, chain="ethereum")
    assert REGISTRY.get_sample_value("lite_indexer_state_last_block", {"chain": "ethereum"}) == 42.0


def test_incr_labeled_counter() -> None:
    before = REGISTRY.get_sample_value("lite_indexer_rpc_failures_total", {"chain": "base"}) or 0.0
    metrics.incr_labeled("lite_indexer_rpc_failures", chain="base")
    after = REGISTRY.get_sample_value("lite_indexer_rpc_failures_total", {"chain": "base"}) or 0.0
    assert after == before + 1.0


def test_webhook_alias_moves_alert_counter() -> None:
    before = REGISTRY.get_sample_value("lite_indexer_webhook_delivery_failed_total") or 0.0
    metrics.incr("webhook_dlq_enqueued")
    after = REGISTRY.get_sample_value("lite_indexer_webhook_delivery_failed_total") or 0.0
    assert after == before + 1.0


# ═══════════════════════════════════════════════════════════════════════
# Gauge export helpers
# ═══════════════════════════════════════════════════════════════════════


def test_export_global_metrics_populates_gauges() -> None:
    export_global_metrics()
    assert REGISTRY.get_sample_value("lite_indexer_rss_bytes") is not None
    assert REGISTRY.get_sample_value("lite_indexer_cgroup_memory_bytes") is not None
    assert REGISTRY.get_sample_value("lite_indexer_rpc_pool_healthy_endpoints", {"chain": "ethereum"}) is not None


def test_export_chain_metrics_populates_state_gauges() -> None:
    class _State:
        def load(self) -> dict[str, Any]:
            return {"last_block": 21_000_000}

    fake = SimpleNamespace(chain="ethereum", state=_State(), _nats_publisher=None)
    export_chain_metrics(fake)  # type: ignore[arg-type]
    assert REGISTRY.get_sample_value("lite_indexer_state_last_block", {"chain": "ethereum"}) == 21_000_000.0
    assert REGISTRY.get_sample_value("lite_indexer_last_block", {"chain": "ethereum"}) == 21_000_000.0


# ═══════════════════════════════════════════════════════════════════════
# HTTP exporter
# ═══════════════════════════════════════════════════════════════════════


def test_start_and_stop_metrics_server() -> None:
    assert metrics_server.start_metrics_server(host="127.0.0.1", port=0) is True
    assert metrics_server.metrics_server_running()
    metrics_server.stop_metrics_server()
    assert not metrics_server.metrics_server_running()


def test_start_is_idempotent() -> None:
    assert metrics_server.start_metrics_server(host="127.0.0.1", port=0) is True
    server = metrics_server._server
    assert metrics_server.start_metrics_server(host="127.0.0.1", port=0) is True
    assert metrics_server._server is server


def test_busy_port_does_not_crash() -> None:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = sock.getsockname()[1]
    try:
        assert metrics_server.start_metrics_server(host="127.0.0.1", port=port) is False
        assert not metrics_server.metrics_server_running()
    finally:
        sock.close()


def test_endpoint_serves_alert_metrics() -> None:
    assert metrics_server.start_metrics_server(host="127.0.0.1", port=0) is True
    address = metrics_server._server.server_address
    port = address[1]
    # bandit B310: loopback to the test's own server  # noqa: S310
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:  # nosec B310
        body = response.read().decode("utf-8")
    assert "lite_indexer_state_last_block" in body
    assert "process_resident_memory_bytes" in body
