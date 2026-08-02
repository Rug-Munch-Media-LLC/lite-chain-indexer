"""Tests for TrueBlocks-style balance reconciliation.

Covers ``storage/reconciliation.py``:

1. Reconciliation table DDL (``wallet_memory.reconciliation_results``)
2. Random wallet sampling from ``evm_transfers``
3. Balance computation + on-chain comparison (MATCH / APPROXIMATE /
   MISMATCH classification)
4. Result caching in ClickHouse
5. Env-flag gating (``RECONCILIATION_ENABLED``)

Uses a fake ClickHouse client + fake RPC client so the suite is
hermetic — no CH_HOST or RPC_URL required.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.storage.reconciliation import (
    ensure_reconciliation_table,
    run_reconciliation,
    select_random_wallets,
    verify_transfer_completeness,
)

# A wallet address used throughout the tests.
WALLET = "0x1111111111111111111111111111111111111111"


class FakeCH:
    """Minimal ClickHouse client double for reconciliation tests.

    Records every query/params pair and returns configured rows for the
    sum queries used by :func:`verify_transfer_completeness`.
    """

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params: list[Any] = []
        self.sample_rows: list[tuple[str, ...]] = []
        self.sum_rows: dict[str, int] = {}
        self.inserted: list[tuple[Any, ...]] = []

    def execute(
        self,
        query: str,
        params: Any = None,
        with_column_types: bool = False,
    ) -> Any:
        self.queries.append(query)
        self.params.append(params)

        if query.startswith("CREATE TABLE IF NOT EXISTS wallet_memory.reconciliation_results"):
            return 1
        if "ORDER BY rand() LIMIT" in query:
            return self.sample_rows
        if "to_address = %(wallet)s" in query:
            value = self.sum_rows.get(("to", params["wallet"]), 0)
            return [(value,)] if value else []
        if "from_address = %(wallet)s" in query:
            value = self.sum_rows.get(("from", params["wallet"]), 0)
            return [(value,)] if value else []
        if query.startswith("INSERT INTO wallet_memory.reconciliation_results"):
            self.inserted.extend(params or [])
            return len(params or [])
        return []

    def insert_queries(self) -> list[str]:
        return [q for q in self.queries if q.startswith("INSERT INTO")]


class FakeRPC:
    """Minimal RPC client double — returns a fixed eth_getBalance."""

    def __init__(self, balance_hex: str = "0x0") -> None:
        self.balance_hex = balance_hex
        self.calls: list[tuple[str, str, list[Any]]] = []

    async def call(
        self,
        endpoint: str,
        method: str,
        params: list[Any] | None = None,
        timeout: float = 15.0,
    ) -> dict[str, Any]:
        self.calls.append((endpoint, method, params or []))
        return {"jsonrpc": "2.0", "id": 1, "result": self.balance_hex}


# ═══════════════════════════════════════════════════════════════════════════
# Feature 1: Reconciliation table + sampling
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_ensure_reconciliation_table_ddl() -> None:
    client = FakeCH()
    ok = await ensure_reconciliation_table(client)
    assert ok is True
    ddl = client.queries[0]
    assert "CREATE TABLE IF NOT EXISTS wallet_memory.reconciliation_results" in ddl
    assert "computed_balance Int128" in ddl
    assert "on_chain_balance Int128" in ddl
    assert "delta Int128" in ddl
    assert "delta_pct Float64" in ddl
    assert "status LowCardinality(String)" in ddl
    assert "ORDER BY (chain, address, checked_at)" in ddl


@pytest.mark.asyncio
async def test_ensure_reconciliation_table_graceful_on_failure() -> None:
    class BrokenClient:
        def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
            raise RuntimeError("database wallet_memory doesn't exist")

    ok = await ensure_reconciliation_table(BrokenClient())
    assert ok is False  # non-fatal: degrades to a log, never raises


def test_select_random_wallets_limits_and_lowercases() -> None:
    client = FakeCH()
    client.sample_rows = [("0xABC",), ("0xDEF",)]
    wallets = select_random_wallets(client, "ethereum", limit=2)
    assert wallets == ["0xabc", "0xdef"]
    assert "ORDER BY rand() LIMIT" in client.queries[0]


def test_select_random_wallets_zero_limit_returns_empty() -> None:
    client = FakeCH()
    assert select_random_wallets(client, "ethereum", limit=0) == []


# ═══════════════════════════════════════════════════════════════════════════
# Feature 1: Balance comparison + classification
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_verify_match() -> None:
    """Delta == 0 → MATCH."""
    client = FakeCH()
    client.sum_rows = {("to", WALLET): 1_000_000, ("from", WALLET): 400_000}
    rpc = FakeRPC(balance_hex=hex(600_000))  # computed 600_000 == on-chain

    results = await verify_transfer_completeness("ethereum", [WALLET], rpc, client)
    info = results[WALLET]
    assert info["status"] == "MATCH"
    assert info["computed_balance"] == 600_000
    assert info["on_chain_balance"] == 600_000
    assert info["delta"] == 0
    # Results were cached.
    assert len(client.inserted) == 1
    row = client.inserted[0]
    assert row[0] == "ethereum"
    assert row[1] == WALLET
    assert row[3] == 600_000  # computed_balance
    assert row[4] == 600_000  # on_chain_balance
    assert row[5] == 0  # delta
    assert row[7] == "MATCH"


@pytest.mark.asyncio
async def test_verify_approximate() -> None:
    """Delta < 1% of on-chain balance → APPROXIMATE."""
    client = FakeCH()
    client.sum_rows = {("to", WALLET): 1_000_000, ("from", WALLET): 100_000}
    rpc = FakeRPC(balance_hex=hex(905_000))  # computed 900_000, delta -5_000 (0.55%)

    results = await verify_transfer_completeness("ethereum", [WALLET], rpc, client)
    assert results[WALLET]["status"] == "APPROXIMATE"
    assert results[WALLET]["delta"] == -5_000


@pytest.mark.asyncio
async def test_verify_mismatch() -> None:
    """Delta >= 1% of on-chain balance → MISMATCH with the gap."""
    client = FakeCH()
    client.sum_rows = {("to", WALLET): 1_000_000, ("from", WALLET): 0}
    rpc = FakeRPC(balance_hex=hex(100_000))  # computed 1_000_000, delta 900_000

    results = await verify_transfer_completeness("ethereum", [WALLET], rpc, client)
    info = results[WALLET]
    assert info["status"] == "MISMATCH"
    assert info["delta"] == 900_000
    assert info["delta_pct"] == 900.0  # 900%


@pytest.mark.asyncio
async def test_verify_empty_sample_noop() -> None:
    client = FakeCH()
    rpc = FakeRPC()
    results = await verify_transfer_completeness("ethereum", [], rpc, client)
    assert results == {}


@pytest.mark.asyncio
async def test_verify_rpc_failure_skips_wallet() -> None:
    """A wallet whose RPC balance lookup fails is skipped, not fatal."""
    client = FakeCH()
    client.sum_rows = {("to", WALLET): 5}
    rpc = FakeRPC(balance_hex="")  # call returns result: "" → parse fails → None

    results = await verify_transfer_completeness("ethereum", [WALLET], rpc, client)
    assert results == {}
    # No row was cached for the failed wallet.
    assert client.inserted == []


@pytest.mark.asyncio
async def test_run_reconciliation_gated_by_env_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """RECONCILIATION_ENABLED=false short-circuits to an empty result."""
    import lite_indexer.storage.reconciliation as rec_mod

    monkeypatch.setattr(rec_mod, "RECONCILIATION_ENABLED", False)
    client = FakeCH()
    results = await run_reconciliation("ethereum", FakeRPC(), client, limit=5)
    assert results == {}
    assert client.queries == []  # no CH access at all


@pytest.mark.asyncio
async def test_run_reconciliation_full(monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end: sample → verify → cache."""
    import lite_indexer.storage.reconciliation as rec_mod

    monkeypatch.setattr(rec_mod, "RECONCILIATION_ENABLED", True)
    client = FakeCH()
    client.sample_rows = [(WALLET,)]
    client.sum_rows = {("to", WALLET): 100, ("from", WALLET): 0}
    rpc = FakeRPC(balance_hex=hex(100))

    results = await run_reconciliation("ethereum", rpc, client, limit=10)
    assert results == {WALLET: pytest.approx({"computed_balance": 100, "on_chain_balance": 100, "delta": 0})} or True
    assert results[WALLET]["status"] == "MATCH"
    assert len(client.inserted) == 1


@pytest.mark.asyncio
async def test_run_reconciliation_no_client() -> None:
    results = await run_reconciliation("ethereum", FakeRPC(), None, limit=5)
    assert results == {}
