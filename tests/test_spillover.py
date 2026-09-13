"""Tests for free→freemium spillover selection (rpc/routing/selector.py).

Policy: free-first must never dead-end. When the preferred tier has no
HEALTHY endpoint, selection escalates to the other tier (the keyed/freemium
overlay) under its account caps. Archive fail-closed triggers ONLY when
neither tier has a healthy archive-capable endpoint.

Hermetic — availability is injected; no network, no disk.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer import metrics
from lite_indexer.rpc import capabilities as cap
from lite_indexer.rpc.capabilities import CapabilityRecord, NoArchiveEndpointError
from lite_indexer.rpc.routing import method_class, select_endpoint_for_method
from lite_indexer.rpc.routing import selector as sel

# Keyless free endpoint and an unresolved-placeholder keyed endpoint (the
# generic, provider-agnostic freemium marker).
FREE = "https://free.example"
KEYED = "https://keyed.example/rpc/${PROVIDER_KEY}"


@pytest.fixture(autouse=True)
def _clean():
    cap._CAPABILITY_RECORDS.clear()
    metrics.reset()
    yield
    cap._CAPABILITY_RECORDS.clear()
    metrics.reset()


def _cap_archive(endpoint: str) -> None:
    cap.set_capability(CapabilityRecord(endpoint=endpoint, chain="ethereum", archive=True))


def _first(calls: list[list[str]]) -> Any:
    def _select(endpoints: list[str]) -> str:
        calls.append(list(endpoints))
        return endpoints[0]

    return _select


class TestMethodClass:
    def test_classes(self):
        assert method_class("eth_getLogs") == "archive"
        assert method_class("trace_filter") == "archive"
        assert method_class("eth_blockNumber") == "tip"


class TestSpillover:
    def test_escalates_to_keyed_when_free_unhealthy(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sel, "_is_endpoint_available", lambda ep: ep == KEYED)
        calls: list[list[str]] = []
        chosen = select_endpoint_for_method("eth_getLogs", [FREE, KEYED], chain="ethereum", base_select=_first(calls))
        assert chosen == KEYED
        assert calls == [[KEYED]]
        assert metrics.get("rpc_spillover") == 1.0

    def test_no_spillover_when_free_healthy(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sel, "_is_endpoint_available", lambda ep: True)
        calls: list[list[str]] = []
        chosen = select_endpoint_for_method("eth_getLogs", [FREE, KEYED], chain="ethereum", base_select=_first(calls))
        assert chosen == FREE
        assert calls == [[FREE]]
        assert metrics.get("rpc_spillover") == 0.0

    def test_tip_method_spills_over_too(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sel, "_is_endpoint_available", lambda ep: ep == KEYED)
        chosen = select_endpoint_for_method("eth_blockNumber", [FREE, KEYED], chain="ethereum")
        assert chosen == KEYED

    def test_fallback_when_neither_tier_healthy(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sel, "_is_endpoint_available", lambda ep: False)
        calls: list[list[str]] = []
        chosen = select_endpoint_for_method("eth_getLogs", [FREE, KEYED], chain="ethereum", base_select=_first(calls))
        # Legacy shape preserved (no dead-end) — pool scoring handles health.
        assert chosen in (FREE, KEYED)
        assert metrics.get("rpc_spillover") == 0.0


class TestArchiveAcrossTiers:
    def test_archive_spillover_to_keyed(self, monkeypatch: pytest.MonkeyPatch):
        _cap_archive(FREE)
        _cap_archive(KEYED)
        monkeypatch.setattr(sel, "_is_endpoint_available", lambda ep: ep == KEYED)
        chosen = select_endpoint_for_method("eth_getLogs", [FREE, KEYED], chain="ethereum")
        assert chosen == KEYED
        assert metrics.get("rpc_spillover") == 1.0

    def test_archive_fail_closed_only_when_neither_tier_healthy(self, monkeypatch: pytest.MonkeyPatch):
        _cap_archive(FREE)
        _cap_archive(KEYED)
        monkeypatch.setattr(sel, "_is_endpoint_available", lambda ep: False)
        with pytest.raises(NoArchiveEndpointError):
            select_endpoint_for_method("eth_getLogs", [FREE, KEYED], chain="ethereum")

    def test_archive_ok_when_keyed_healthy(self, monkeypatch: pytest.MonkeyPatch):
        _cap_archive(FREE)
        _cap_archive(KEYED)
        seen: list[str] = []

        def _avail(ep: str) -> bool:
            seen.append(ep)
            return ep == KEYED

        monkeypatch.setattr(sel, "_is_endpoint_available", _avail)
        chosen = select_endpoint_for_method("eth_getProof", [FREE, KEYED], chain="ethereum")
        assert chosen == KEYED
