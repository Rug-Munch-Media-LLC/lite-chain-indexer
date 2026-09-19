"""Tests for idempotent transfer writes (reconciliation-rewind dedup).

A re-extracted range must not append duplicate rows. The writer dedups
within the batch and skips (chain, block_number, tx_hash, log_index) keys
already present in ClickHouse. Hermetic — no network, no ClickHouse.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.storage import clickhouse as ch


class FakeClient:
    """Minimal ClickHouse client: records INSERTs, answers the dedup SELECT."""

    def __init__(self, existing: list[tuple[Any, ...]] | None = None) -> None:
        self.existing = existing or []
        self.inserts: list[tuple[str, Any]] = []
        self.dedup_queries: list[dict[str, Any]] = []

    def execute(self, query: str, params: Any = None) -> Any:
        if query.strip().upper().startswith("INSERT"):
            self.inserts.append((query, params))
            return None
        if "SELECT block_number, tx_hash, log_index" in query:
            self.dedup_queries.append(params)
            return self.existing
        return []


def _event(block: int, tx: str, idx: int) -> dict[str, Any]:
    return {
        "blockNumber": block,
        "txHash": tx,
        "logIndex": idx,
        "from": "TFrom",
        "to": "TTo",
        "tokenAddress": "TToken",
        "value": "1000",
        "blockTimestamp": "2023-11-14T22:13:20Z",
    }


def _writer(monkeypatch, existing=None) -> tuple[ch.ClickHouseWriter, FakeClient]:
    monkeypatch.setattr(ch, "CH_TTL_ENABLED", False)
    writer = ch.ClickHouseWriter()
    client = FakeClient(existing=existing)
    writer._client = client
    return writer, client


def test_write_transfers_skips_rows_already_in_clickhouse(monkeypatch) -> None:
    writer, client = _writer(monkeypatch, existing=[(100, "0xaa", 0)])
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0), _event(100, "0xbb", 1)])

    assert written == 1
    assert len(client.inserts) == 1
    rows = client.inserts[0][1]
    assert len(rows) == 1
    assert rows[0][1] == 100  # block_number
    assert rows[0][2] == "0xbb"  # tx_hash
    assert rows[0][8] == 1  # log_index
    assert rows[0][11] == "100:0xbb:1"  # tx_log_key (storage dedup key)


def test_tx_log_key_matches_app_dedup_key(monkeypatch) -> None:
    """The storage key must equal the app dedup key (block, tx_hash, log)."""
    writer, client = _writer(monkeypatch)
    writer.write_transfers("tron", [_event(100, "0xaa", 0)])

    rows = client.inserts[0][1]
    assert rows[0][11] == "100:0xaa:0"
    # Ordering contract: block:tx_hash:log_index — same as _existing_transfer_keys.
    assert rows[0][11].split(":") == ["100", "0xaa", "0"]


def test_tx_log_key_distinguishes_same_log_index_different_tx(monkeypatch) -> None:
    """Same (block, log_index) but different tx_hash => distinct dedup keys."""
    writer, client = _writer(monkeypatch)
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0), _event(100, "0xbb", 0)])

    assert written == 2
    keys = {r[11] for r in client.inserts[0][1]}
    assert keys == {"100:0xaa:0", "100:0xbb:0"}


def test_migration_018_declares_dedup_key_contract() -> None:
    """Schema contract: migration 018 appends tx_log_key to the sorting key.

    Guards against a regression to migration 017's invalid form (appending the
    EXISTING tx_hash column), which ClickHouse 26.7 rejects with Code 36.
    """
    from pathlib import Path

    mig = Path(__file__).resolve().parents[1] / "migrations"
    text = (mig / "018_evm_transfers_dedup_key_v2.sql").read_text()
    assert "MODIFY ORDER BY (chain, block_number, log_index, tx_log_key)" in text
    # CH 26.7 accepts a new key column only when it is ADDed in the same ALTER.
    # The runtime fallback may pre-create tx_log_key, so the migration MUST
    # DROP-then-ADD (a bare `ADD COLUMN IF NOT EXISTS` leaves it existing and
    # the MODIFY ORDER BY then fails with Code 36 — the 017 failure mode).
    assert "DROP COLUMN IF EXISTS tx_log_key" in text
    assert "ADD COLUMN tx_log_key String" in text
    old = (mig / "017_evm_transfers_dedup_key.sql").read_text()
    assert "INVALID" in old and "SUPERSEDED" in old and "DO NOT APPLY" in old


def test_write_transfers_dedups_identical_events_in_batch(monkeypatch) -> None:
    writer, client = _writer(monkeypatch)
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0), _event(100, "0xaa", 0)])

    assert written == 1
    rows = client.inserts[0][1]
    assert len(rows) == 1


def test_write_transfers_all_duplicates_writes_nothing(monkeypatch) -> None:
    writer, client = _writer(monkeypatch, existing=[(100, "0xaa", 0)])
    written = writer.write_transfers("tron", [_event(100, "0xaa", 0)])

    assert written == 0
    assert client.inserts == []
