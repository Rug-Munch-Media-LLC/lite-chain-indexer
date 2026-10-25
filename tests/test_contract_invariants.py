"""Contract invariants that the module split must never weaken.

1. An unconfirmed block is never persisted to the durable L2 cache, and a
   single endpoint advertising a higher head can never lower that bar
   (the cursor may only advance on confirmed data).
2. A historical (archive-class) read never lands on a tip-only / free-tier
   endpoint — it fails closed instead.
"""

from __future__ import annotations

import pytest

from lite_indexer.rpc.archive_routing import NoArchiveEndpointError
from lite_indexer.rpc.respcache import _l2_writable, _Scope, note_head
from lite_indexer.rpc.routing import selector
from lite_indexer.rpc.routing.selector import select_archive_endpoint


@pytest.fixture()
def heads(monkeypatch):
    store: dict[str, int] = {}
    monkeypatch.setattr("lite_indexer.rpc.respcache._heads", store)
    return store


def _range_scope(to_block: int) -> _Scope:
    return _Scope(ttl=60.0, key="k", from_block=to_block - 10, to_block=to_block)


class TestUnconfirmedNeverPersisted:
    def test_near_tip_read_is_not_durable(self, heads):
        assert _l2_writable(_range_scope(1000), "eth", tip=1000) is False

    def test_read_at_confirm_depth_is_durable(self, heads):
        from lite_indexer.config import RPC_CACHE_CONFIRMATIONS

        deep = 1000 - RPC_CACHE_CONFIRMATIONS
        assert _l2_writable(_range_scope(deep), "eth", tip=1000) is True

    def test_unknown_tip_is_never_durable(self, heads):
        assert _l2_writable(_range_scope(500), "eth", tip=None) is False

    def test_monotonic_head_beats_an_ahead_endpoint(self, heads):
        """A single endpoint reporting a higher tip must not make data durable."""
        note_head("eth", 1000)
        # caller believes the tip is 2000 (stale/ahead endpoint) — the pool's
        # monotonic head still rules, so 1050 stays unconfirmed.
        assert _l2_writable(_range_scope(1050), "eth", tip=2000) is False

    def test_hash_scoped_read_is_immutable_by_content(self, heads):
        scope = _Scope(ttl=60.0, key="k", block_hash="0xabc")
        assert _l2_writable(scope, "eth", tip=None) is True

    def test_identity_read_is_always_durable(self, heads):
        assert _l2_writable(_Scope(ttl=60.0, key="k"), "eth", tip=None) is True


class TestFreeTierNeverServesHistory:
    def _method(self) -> str:
        from lite_indexer.rpc.capabilities import method_requires_archive

        for candidate in ("eth_getStorageAt", "eth_getBalance", "eth_call", "eth_getLogs"):
            if method_requires_archive(candidate):
                return candidate
        pytest.skip("no archive-class method available in this build")

    def test_tip_only_pool_fails_closed(self, monkeypatch):
        monkeypatch.setattr(selector, "get_capability", lambda ep: object())
        monkeypatch.setattr(selector, "is_archive_capable", lambda ep, method: False)
        monkeypatch.setattr(selector, "_is_endpoint_available", lambda ep: True)
        with pytest.raises(NoArchiveEndpointError):
            select_archive_endpoint(self._method(), ["https://free.example"], chain="eth")

    def test_archive_endpoint_is_returned_when_present(self, monkeypatch):
        monkeypatch.setattr(selector, "get_capability", lambda ep: object())
        monkeypatch.setattr(selector, "is_archive_capable", lambda ep, method: ep.startswith("https://archive"))
        monkeypatch.setattr(selector, "_is_endpoint_available", lambda ep: True)
        chosen = select_archive_endpoint(
            self._method(),
            ["https://free.example", "https://archive.example"],
            chain="eth",
        )
        assert chosen == "https://archive.example"

    def test_non_archive_method_is_rejected(self, monkeypatch):
        monkeypatch.setattr(selector, "get_capability", lambda ep: object())
        with pytest.raises(ValueError):
            select_archive_endpoint("eth_blockNumber", ["https://free.example"], chain="eth")
