"""Tests for Envio/TrueBlocks-style reorg staging.

Covers ``strategies/evm/reorg.py`` plus the ``EVMChainStrategy.reorg_stage``
wiring:

1. Checkpoint table DDL (``wallet_memory.block_checkpoints``)
2. ``checkpoint_block`` — records a block hash
3. ``detect_reorg`` — compares current hash vs checkpoint
4. ``rollback_to_block`` — DELETEs rows above the fork point
5. Strategy ``reorg_stage`` — checkpoint + verify + rollback on mismatch
6. Env-flag gating (``REORG_PROTECTION_ENABLED``)

Uses a fake ClickHouse client and a fake RPC so the suite is hermetic.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.strategies.evm.reorg import (
    MAX_REORG_DEPTH,
    checkpoint_block,
    detect_reorg,
    ensure_checkpoint_table,
    reorg_depth,
    rollback_to_block,
)

HASH_A = "0x" + "a" * 64
HASH_B = "0x" + "b" * 64


class FakeCH:
    """Minimal ClickHouse client double for reorg tests.

    Stores block checkpoints in memory keyed by (chain, block) and
    records every DELETE issued by the rollback.
    """

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.params: list[Any] = []
        self.checkpoints: dict[tuple[str, int], str] = {}
        self.deletes: list[tuple[str, int]] = []

    def execute(
        self,
        query: str,
        params: Any = None,
        with_column_types: bool = False,
    ) -> Any:
        self.queries.append(query)
        self.params.append(params)

        if query.startswith("CREATE TABLE IF NOT EXISTS wallet_memory.block_checkpoints"):
            return 1
        if query.startswith("INSERT INTO wallet_memory.block_checkpoints"):
            for row in params or []:
                self.checkpoints[(row[0], row[1])] = row[2]
            return len(params or [])
        if "SELECT block_hash FROM wallet_memory.block_checkpoints" in query:
            key = (params["chain"], params["block"])
            stored = self.checkpoints.get(key)
            return [(stored,)] if stored else []
        if query.startswith("DELETE FROM"):
            # Parse the fork block out of the WHERE clause.
            fork = int(params["fork"])
            table = "evm_transfers" if "evm_transfers" in query else "dex_events"
            self.deletes.append((table, fork))
            # Also prune checkpoints above the fork on the checkpoint table.
            if "block_checkpoints" in query:
                self.checkpoints = {k: v for k, v in self.checkpoints.items() if k[1] <= fork}
            return 3
        return []


class FakeRPC:
    """RPC double returning configurable block hashes by number."""

    def __init__(self, hashes: dict[int, str] | None = None) -> None:
        self.hashes = hashes or {}
        self.requests: list[int] = []

    async def eth_get_block_by_number(
        self,
        endpoint: str,
        block_number: int,
        full_tx: bool = False,
    ) -> dict[str, Any] | None:
        self.requests.append(block_number)
        block_hash = self.hashes.get(block_number)
        if block_hash is None:
            return None
        return {"number": hex(block_number), "hash": block_hash}


# ═══════════════════════════════════════════════════════════════════════════
# Reorg module primitives
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_ensure_checkpoint_table_ddl() -> None:
    client = FakeCH()
    ok = await ensure_checkpoint_table(client)
    assert ok is True
    ddl = client.queries[0]
    assert "CREATE TABLE IF NOT EXISTS wallet_memory.block_checkpoints" in ddl
    assert "block_number UInt64" in ddl
    assert "block_hash String" in ddl
    assert "ORDER BY (chain, block_number)" in ddl


@pytest.mark.asyncio
async def test_ensure_checkpoint_table_graceful_on_failure() -> None:
    class BrokenClient:
        def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
            raise RuntimeError("database wallet_memory doesn't exist")

    ok = await ensure_checkpoint_table(BrokenClient())
    assert ok is False


@pytest.mark.asyncio
async def test_checkpoint_block_records_hash() -> None:
    client = FakeCH()
    ok = await checkpoint_block("ethereum", 1_000_000, HASH_A, client)
    assert ok is True
    assert client.checkpoints[("ethereum", 1_000_000)] == HASH_A
    assert "INSERT INTO wallet_memory.block_checkpoints" in client.queries[0]


@pytest.mark.asyncio
async def test_checkpoint_block_empty_hash_noop() -> None:
    client = FakeCH()
    ok = await checkpoint_block("ethereum", 1_000_000, "", client)
    assert ok is False
    assert client.queries == []


@pytest.mark.asyncio
async def test_detect_reorg_mismatch_true() -> None:
    client = FakeCH()
    client.checkpoints[("ethereum", 1_000_000)] = HASH_A
    # Current chain hash differs → reorg.
    assert await detect_reorg("ethereum", 1_000_000, HASH_B, client) is True


@pytest.mark.asyncio
async def test_detect_reorg_match_false() -> None:
    client = FakeCH()
    client.checkpoints[("ethereum", 1_000_000)] = HASH_A
    assert await detect_reorg("ethereum", 1_000_000, HASH_A, client) is False


@pytest.mark.asyncio
async def test_detect_reorg_no_checkpoint_false() -> None:
    """No checkpoint yet → cannot detect a reorg → False."""
    client = FakeCH()
    assert await detect_reorg("ethereum", 1_000_000, HASH_B, client) is False


@pytest.mark.asyncio
async def test_detect_reorg_empty_hash_false() -> None:
    """An empty current hash cannot be compared → False (no reorg)."""
    client = FakeCH()
    client.checkpoints[("ethereum", 1_000_000)] = HASH_A
    assert await detect_reorg("ethereum", 1_000_000, "", client) is False
    assert client.queries == []  # no CH lookup attempted


@pytest.mark.asyncio
async def test_detect_reorg_lookup_failure_false() -> None:
    """A ClickHouse lookup failure degrades to 'no reorg' (never raises)."""

    class BrokenClient:
        def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
            raise RuntimeError("connection refused")

    assert await detect_reorg("ethereum", 1_000_000, HASH_B, BrokenClient()) is False


@pytest.mark.asyncio
async def test_detect_reorg_hash_comparison_is_case_insensitive() -> None:
    """Stored vs current hashes are compared lowercased."""
    client = FakeCH()
    client.checkpoints[("ethereum", 1_000_000)] = HASH_A.upper()
    assert await detect_reorg("ethereum", 1_000_000, HASH_A, client) is False


@pytest.mark.asyncio
async def test_checkpoint_block_failure_returns_false() -> None:
    class BrokenClient:
        def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
            raise RuntimeError("table missing")

    assert await checkpoint_block("ethereum", 100, HASH_A, BrokenClient()) is False


@pytest.mark.asyncio
async def test_rollback_to_block_deletes_above_fork() -> None:
    client = FakeCH()
    client.checkpoints[("ethereum", 100)] = HASH_A
    client.checkpoints[("ethereum", 150)] = HASH_A

    deleted = await rollback_to_block("ethereum", 120, client)

    assert deleted == 6  # 3 + 3 from the two data tables
    assert ("evm_transfers", 120) in client.deletes
    assert ("dex_events", 120) in client.deletes
    # Checkpoints above the fork were pruned; the fork point stays.
    assert client.checkpoints == {("ethereum", 100): HASH_A}


@pytest.mark.asyncio
async def test_rollback_partial_table_failure_counts_survivors() -> None:
    """A failing table is logged; the surviving tables still count."""

    class PartialClient(FakeCH):
        def execute(
            self,
            query: str,
            params: Any = None,
            with_column_types: bool = False,
        ) -> Any:
            if "DELETE FROM wallet_memory.evm_transfers" in query:
                raise RuntimeError("privileges")
            return super().execute(query, params, with_column_types=with_column_types)

    client = PartialClient()
    deleted = await rollback_to_block("ethereum", 120, client)
    # dex_events (3) + checkpoint prune path; evm_transfers failed.
    assert deleted == 3
    assert ("dex_events", 120) in client.deletes
    assert ("evm_transfers", 120) not in client.deletes


def test_max_reorg_depth_per_chain() -> None:
    assert MAX_REORG_DEPTH["ethereum"] == 12
    assert MAX_REORG_DEPTH["polygon"] == 32
    assert MAX_REORG_DEPTH["bsc"] == 12
    assert reorg_depth("ethereum") == 12
    assert reorg_depth("polygon") == 32
    assert reorg_depth("unknown-chain") == 12  # fallback


# ═══════════════════════════════════════════════════════════════════════════
# Strategy wiring — EVMChainStrategy.reorg_stage
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_reorg_stage_no_reorg_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from lite_indexer.strategies.evm import EVMChainStrategy

    monkeypatch.setattr("lite_indexer.strategies.evm.strategy.REORG_PROTECTION_ENABLED", True)

    # Tip (200) checkpointed; verify block (188 = 200 - 12) hash matches.
    rpc = FakeRPC({200: HASH_A, 188: HASH_A})
    client = FakeCH()
    client.checkpoints[("ethereum", 188)] = HASH_A  # checkpointed earlier

    strategy = EVMChainStrategy("ethereum", rpc=rpc)
    fork = await strategy.reorg_stage(100, 200, client)

    assert fork is None
    # Tip was checkpointed.
    assert client.checkpoints[("ethereum", 200)] == HASH_A
    # No rollback issued.
    assert client.deletes == []


@pytest.mark.asyncio
async def test_reorg_stage_detects_and_rolls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    from lite_indexer.strategies.evm import EVMChainStrategy

    monkeypatch.setattr("lite_indexer.strategies.evm.strategy.REORG_PROTECTION_ENABLED", True)

    # The block that should be deep-confirmed (188) changed its hash: reorg.
    rpc = FakeRPC({200: HASH_A, 188: HASH_B})
    client = FakeCH()
    client.checkpoints[("ethereum", 188)] = HASH_A  # what we indexed earlier

    strategy = EVMChainStrategy("ethereum", rpc=rpc)
    fork = await strategy.reorg_stage(100, 200, client)

    assert fork == 188  # re-fetch from fork point
    assert ("evm_transfers", 188) in client.deletes
    assert ("dex_events", 188) in client.deletes


@pytest.mark.asyncio
async def test_reorg_stage_disabled_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from lite_indexer.strategies.evm import EVMChainStrategy

    monkeypatch.setattr("lite_indexer.strategies.evm.strategy.REORG_PROTECTION_ENABLED", False)

    rpc = FakeRPC({200: HASH_A, 188: HASH_B})
    client = FakeCH()
    strategy = EVMChainStrategy("ethereum", rpc=rpc)
    fork = await strategy.reorg_stage(100, 200, client)

    assert fork is None
    assert client.queries == []  # no CH access when disabled


@pytest.mark.asyncio
async def test_reorg_stage_no_ch_client_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from lite_indexer.strategies.evm import EVMChainStrategy

    monkeypatch.setattr("lite_indexer.strategies.evm.strategy.REORG_PROTECTION_ENABLED", True)
    strategy = EVMChainStrategy("ethereum", rpc=FakeRPC({200: HASH_A, 188: HASH_B}))
    fork = await strategy.reorg_stage(100, 200, None)
    assert fork is None


@pytest.mark.asyncio
async def test_reorg_stage_no_endpoints_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    from lite_indexer.strategies.evm import EVMChainStrategy

    monkeypatch.setattr("lite_indexer.strategies.evm.strategy.REORG_PROTECTION_ENABLED", True)
    strategy = EVMChainStrategy("not-a-real-chain", rpc=FakeRPC({200: HASH_A}))
    fork = await strategy.reorg_stage(100, 200, FakeCH())
    assert fork is None
