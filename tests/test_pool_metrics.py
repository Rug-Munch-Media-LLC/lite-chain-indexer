"""Producer-side tests for the honest RPC-pool gauges (2026-10-23).

A working-but-slow endpoint must read healthy with its TRUE span; a
pruned/403 endpoint must read unhealthy. And a real getLogs probe must
persist a capability record even for a keyed URL — without it the span is
unknown, the gauge reads 0 and every request is judged a violation.
"""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY

from lite_indexer.rpc.capabilities import (
    CapabilityRecord,
    clear_capabilities,
    get_capability,
    set_capability,
)
from lite_indexer.rpc.health import clear_health_state
from lite_indexer.rpc.health_logs import clear_logs_verdicts
from lite_indexer.rpc.logs_capability_persist import note_logs_probe
from lite_indexer.rpc.pool_metrics import publish_pool_metrics, usable_span

KEYED_BSC = "https://rpc.ankr.com/bsc/test-key-123"
SLOW_BSC = "https://bsc.blockrazor.xyz"
PRUNED = "https://bsc-rpc.publicnode.com"


@pytest.fixture(autouse=True)
def _clean_gating_state() -> None:
    """Isolate capability / health / probe-verdict registries per test."""
    clear_capabilities()
    clear_health_state()
    clear_logs_verdicts()
    yield
    clear_capabilities()
    clear_health_state()
    clear_logs_verdicts()


def _gauge(name: str, chain: str) -> float:
    value = REGISTRY.get_sample_value(name, {"chain": chain})
    assert value is not None, (name, chain)
    return value


def test_working_slow_endpoint_reads_healthy_with_true_span() -> None:
    """A 20-block endpoint is capacity 20, not zero (the bsc lie)."""
    set_capability(CapabilityRecord(endpoint=SLOW_BSC, chain="bsc", logs_capable=None, getlogs_max_range=20))
    publish_pool_metrics({"bsc": [SLOW_BSC]})

    assert usable_span(SLOW_BSC) == 20
    assert _gauge("lite_indexer_rpc_pool_healthy_endpoints", "bsc") == 1
    assert _gauge("lite_indexer_rpc_pool_max_span", "bsc") == 20
    assert _gauge("lite_indexer_rpc_pool_usable_span", "bsc") == 20


def test_pruned_or_403_endpoint_reads_unhealthy() -> None:
    """A refused endpoint counts as zero even though it answers liveness."""
    set_capability(CapabilityRecord(endpoint=PRUNED, chain="bsc", logs_capable=False, getlogs_max_range=0))
    publish_pool_metrics({"bsc": [PRUNED]})

    assert usable_span(PRUNED) == 0
    assert _gauge("lite_indexer_rpc_pool_healthy_endpoints", "bsc") == 0


def test_keyed_url_real_probe_persists_capability_and_span() -> None:
    """set_capability must run for keyed URLs proven by a real getLogs probe."""
    assert note_logs_probe(KEYED_BSC, "bsc", 5, True, "ok") is True

    record = get_capability(KEYED_BSC)
    assert record is not None
    assert record.logs_capable is True
    assert record.getlogs_max_range == 5
    assert record.chain == "bsc"
    assert record.probed_at > 0


def test_probe_success_never_shrinks_a_measured_span() -> None:
    """A 5-block probe on a 20-block endpoint keeps the 20-block span."""
    note_logs_probe(SLOW_BSC, "bsc", 20, True, "ok")
    note_logs_probe(SLOW_BSC, "bsc", 5, True, "ok")

    record = get_capability(SLOW_BSC)
    assert record is not None
    assert record.getlogs_max_range == 20


def test_capability_refusal_marks_unhealthy_transient_does_not() -> None:
    """403/-32701/pruned exclusions are recorded; a timeout never excludes."""
    assert note_logs_probe(PRUNED, "bsc", 5, False, "http 403") is True
    assert get_capability(PRUNED) is not None
    assert get_capability(PRUNED).logs_capable is False

    assert note_logs_probe(SLOW_BSC, "bsc", 5, False, "transport: ReadTimeout") is False
    record = get_capability(SLOW_BSC)
    assert record is None or record.logs_capable is not False


def test_gauge_counts_only_usable_endpoints() -> None:
    """One working endpoint + one pruned endpoint reads 1, span 20."""
    set_capability(CapabilityRecord(endpoint=SLOW_BSC, chain="bsc", logs_capable=None, getlogs_max_range=20))
    set_capability(CapabilityRecord(endpoint=PRUNED, chain="bsc", logs_capable=False))
    publish_pool_metrics({"bsc": [SLOW_BSC, PRUNED]})

    assert _gauge("lite_indexer_rpc_pool_healthy_endpoints", "bsc") == 1
    assert _gauge("lite_indexer_rpc_pool_max_span", "bsc") == 20
    assert _gauge("lite_indexer_rpc_pool_usable_span", "bsc") == 20
