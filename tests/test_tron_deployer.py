"""Unit tests for tron deployer-events coverage (strategies/tron/deployer.py).

Offline — fake ClickHouse client, no live network. Covers:

1. ``to_deployer_rows`` — LaunchEvent → creator-as-deployer row,
   PairEvent → empty deployer (never mislabeled with the factory),
   unknown event types skipped.
2. ``write_tron_deployer_events`` — insert shape matches the
   SCANNER_CONTRACT.md ``deployer_events`` columns; empty input is a
   no-op; ClickHouse failures are contained (return 0, no raise).
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.strategies.tron.deployer import to_deployer_rows, write_tron_deployer_events
from lite_indexer.strategies.tron.launch import SUNPUMP_CONTRACT, SUNSWAP_FACTORY, LaunchEvent, PairEvent

TOKEN = "TUpauSxrr4EAnLQvfUwqDkN9rfGmSshpiu"
CREATOR = "TQrY8tryqkDGrD7NQvTNPrHPLbrynU5fCi"
PAIR = "TXsSmhwpJmTZK1YKvBQwvUZqUWjrA3uFqk"


class FakeClient:
    """ClickHouse double capturing execute() calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[Any]]] = []

    def execute(self, sql: str, rows: list[Any]) -> None:
        self.calls.append((sql, rows))


class FakeWriter:
    """Duck-typed ClickHouseWriter exposing client()."""

    def __init__(self, client: FakeClient | None) -> None:
        self._client = client

    def client(self) -> FakeClient | None:
        return self._client


class ExplodingClient:
    def execute(self, sql: str, rows: list[Any]) -> None:
        raise RuntimeError("clickhouse down")


def _launch() -> LaunchEvent:
    return LaunchEvent(
        token_address=TOKEN,
        creator=CREATOR,
        token_index=7,
        tx_hash="0xabc123",
        block_number=42_000_000,
        log_index=3,
        block_timestamp="2026-09-20T10:00:00Z",
    )


def _pair() -> PairEvent:
    return PairEvent(
        pair_address=PAIR,
        token0=TOKEN,
        token1=CREATOR,
        tx_hash="0xdef456",
        block_number=42_000_001,
        log_index=1,
        block_timestamp="2026-09-20T10:00:03Z",
    )


class TestToDeployerRows:
    def test_launch_row_maps_creator_as_deployer(self):
        rows = to_deployer_rows([_launch()])
        assert len(rows) == 1
        r = rows[0]
        assert r[0] == CREATOR  # deployer
        assert r[1] == CREATOR  # deployer_eoa
        assert r[2] == TOKEN  # token_address
        assert r[3] == TOKEN  # token0
        assert r[4] == ""  # token1
        assert r[5] == ""  # pair_address
        assert r[6] == "tron"
        assert r[7] == 42_000_000
        assert r[9] == "0xabc123"
        assert r[10] == "sunpump_launch"
        assert r[11] == SUNPUMP_CONTRACT  # factory
        assert r[13] == "lite_indexer"  # source

    def test_pair_row_keeps_deployer_empty(self):
        rows = to_deployer_rows([_pair()])
        r = rows[0]
        assert r[0] == ""  # deployer — never the factory
        assert r[1] == ""
        assert r[2] == TOKEN  # token_address ← token0
        assert r[3] == TOKEN
        assert r[4] == CREATOR
        assert r[5] == PAIR
        assert r[10] == "sunswap_pair"
        assert r[11] == SUNSWAP_FACTORY

    def test_unknown_event_type_skipped(self):
        rows = to_deployer_rows(["junk", _launch()])  # type: ignore[list-item]
        assert len(rows) == 1

    def test_empty_input(self):
        assert to_deployer_rows([]) == []

    def test_bad_timestamp_falls_back(self):
        evt = _launch()
        evt.block_timestamp = "not-a-date"
        rows = to_deployer_rows([evt])
        assert rows[0][8] is not None  # datetime fallback, no raise


class TestWriteTronDeployerEvents:
    async def test_inserts_contract_columns(self):
        client = FakeClient()
        written = await write_tron_deployer_events("tron", [_launch(), _pair()], FakeWriter(client))
        assert written == 2
        sql, rows = client.calls[0]
        assert "wallet_memory.deployer_events" in sql
        for col in ("deployer", "deployer_eoa", "token_address", "pair_address", "factory", "source"):
            assert col in sql
        assert len(rows) == 2

    async def test_empty_events_noop(self):
        client = FakeClient()
        assert await write_tron_deployer_events("tron", [], FakeWriter(client)) == 0
        assert client.calls == []

    async def test_failure_contained(self):
        written = await write_tron_deployer_events("tron", [_launch()], FakeWriter(ExplodingClient()))
        assert written == 0

    async def test_none_client_noop(self):
        assert await write_tron_deployer_events("tron", [_launch()], FakeWriter(None)) == 0


@pytest.mark.parametrize("evt_factory,expected_type", [(_launch, "sunpump_launch"), (_pair, "sunswap_pair")])
async def test_roundtrip_event_type(evt_factory: Any, expected_type: str):
    rows = to_deployer_rows([evt_factory()])
    assert rows[0][10] == expected_type
