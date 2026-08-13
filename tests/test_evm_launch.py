"""Hermetic tests for minimal EVM launch detection.

Covers the mapping + write path of
:mod:`lite_indexer.strategies.evm.launch` and the hook in
:mod:`lite_indexer.indexer._writers` that mirrors verified pair-created
events into ``wallet_memory.evm_launches``. No ClickHouse or RPC needed —
the writer is exercised with a duck-typed client that records ``execute``
calls.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lite_indexer.strategies.evm.launch import (
    EVM_LAUNCHES_TABLE,
    ensure_evm_launches_table,
    write_launch,
)

_TS_STR = "2026-08-13T12:00:00Z"


class FakeClient:
    """Records execute() calls (DDL + INSERTs) without a live ClickHouse."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, list[Any]]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))


class FakeWriter:
    """Duck-typed ClickHouseWriter exposing ``client()`` (as the launch writers expect)."""

    def __init__(self, client: FakeClient) -> None:
        self._client = client

    def client(self) -> FakeClient:
        return self._client


class FakeNats:
    """Records publish_event() calls."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, dict[str, Any]]] = []

    async def publish_event(self, chain: str, event_type: str, payload: dict[str, Any]) -> None:
        self.published.append((chain, event_type, payload))


def _pair_event() -> dict[str, Any]:
    """Enriched deployer-event shape produced by the pair-created path."""
    return {
        "factory": "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
        "pair_address": "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852",
        "token0": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "token1": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "version": "v2",
        "blockNumber": 19_000_001,
        "transactionHash": "0xabc123",
        "blockTimestamp": _TS_STR,
    }


async def test_write_launch_inserts_mapped_row() -> None:
    client = FakeClient()
    writer = FakeWriter(client)
    nats = FakeNats()

    await write_launch("ethereum", _pair_event(), writer, nats)

    inserts = [c for c in client.executed if c[0].startswith("INSERT")]
    assert len(inserts) == 1
    sql, rows = inserts[0]
    assert EVM_LAUNCHES_TABLE in sql
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == "ethereum"  # chain
    assert row[1] == 19_000_001  # block_number
    assert isinstance(row[2], datetime)  # block_timestamp parsed
    assert row[2].year == 2026
    assert row[3] == "0xabc123"  # tx_hash
    assert row[4] == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # token0
    assert row[5] == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # token1
    assert row[6] == "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"  # pair_address
    assert row[7] == "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"  # factory
    assert row[8] == "v2"  # version
    assert row[9] == "lite_indexer"  # source

    # NATS fan-out on the same 'launch' subject as Solana/Tron.
    assert len(nats.published) == 1
    chain, event_type, payload = nats.published[0]
    assert chain == "ethereum"
    assert event_type == "launch"
    assert payload["pair_address"] == "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"
    assert payload["event_type"] == "pair_created"
    assert payload["confidence"] == "high"


async def test_write_launch_best_effort_on_insert_error() -> None:
    class BoomClient(FakeClient):
        def execute(self, sql: str, params: Any = None) -> None:
            raise RuntimeError("connection refused")

    await write_launch("base", _pair_event(), FakeWriter(BoomClient()))
    # Must not raise — ClickHouse failures are logged and swallowed.


def test_ensure_table_is_idempotent() -> None:
    client = FakeClient()
    assert ensure_evm_launches_table(client) is True
    assert ensure_evm_launches_table(client) is True
    ddl_calls = [c for c in client.executed if "CREATE TABLE IF NOT EXISTS" in c[0]]
    assert len(ddl_calls) == 2  # DDL is re-run idempotently (IF NOT EXISTS)


async def test_writers_hook_mirrors_pair_events_to_evm_launches() -> None:
    """write_deployer_events writes evm_launches rows from the same events."""
    from lite_indexer.indexer._writers import write_deployer_events

    client = FakeClient()

    class FakeDeployerWriter(FakeWriter):
        """FakeWriter + the deployer write method the hook calls."""

        def __init__(self) -> None:
            super().__init__(client)
            self.deployer_calls: list[tuple[str, list[dict[str, Any]]]] = []

        async def write_deployer_events(self, chain: str, events: list[dict[str, Any]]) -> int:
            self.deployer_calls.append((chain, events))
            return len(events)

    writer = FakeDeployerWriter()

    class FakeEVMStrategy:
        def __init__(self, events: list[dict[str, Any]]) -> None:
            self._last_pair_events = events

    async def enrich(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return events

    events = [_pair_event(), dict(_pair_event(), pair_address="0xsecondpair", transactionHash="0xdef456")]
    strategy: Any = FakeEVMStrategy(events)

    written = await write_deployer_events("ethereum", strategy, None, writer, enrich)  # type: ignore[arg-type]

    assert written == 2
    assert writer.deployer_calls[0][0] == "ethereum"
    inserts = [c for c in client.executed if c[0].startswith("INSERT")]
    assert len(inserts) == 2  # one launch row per verified pair event
    pairs = {rows[0][6] for _, rows in inserts}
    assert pairs == {"0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852", "0xsecondpair"}


async def test_writers_hook_noop_without_evm_strategy() -> None:
    """Non-EVM chains never take the evm_launches path."""
    from lite_indexer.indexer._writers import write_deployer_events

    client = FakeClient()
    writer = FakeWriter(client)

    class FakeSolanaStrategy:
        _last_deployer_events: list[dict[str, Any]] = []

    solana_strategy: Any = FakeSolanaStrategy()

    async def enrich(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return events

    await write_deployer_events("solana", None, solana_strategy, writer, enrich)  # type: ignore[arg-type]
    assert not any(c[0].startswith("INSERT") for c in client.executed)
