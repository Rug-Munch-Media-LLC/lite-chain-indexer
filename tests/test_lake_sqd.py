"""Tests for the SQD block-range fill seam (DI only, no network, no monkeypatch)."""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.ingest.lake_sqd import (
    LOG_SELECTORS,
    SqdFillRequest,
    SqdIngestSource,
    fill_chain_from_sqd,
    map_sqd_transfer,
)
from lite_indexer.query.sqd_source import ERC20_TRANSFER_TOPIC

_PAD = "0" * 24
_DATA_1000 = "0x" + format(1000, "064x")
_TS = 1789551829


def _log(
    topic0: str = ERC20_TRANSFER_TOPIC, *, data: str = _DATA_1000, topics: list[str] | None = None
) -> dict[str, Any]:
    if topics is None:
        topics = [topic0, "0x" + _PAD + "aa" * 20, "0x" + _PAD + "bb" * 20]
    return {"address": "0xToken", "topics": topics, "data": data, "logIndex": 7, "transactionIndex": 1}


def _block(number: int, logs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"header": {"number": number, "timestamp": _TS}, "logs": logs}


# ── Mapping tests (fixture dicts, no network) ─────────────────────────


def test_map_sqd_transfer_decodes() -> None:
    row = map_sqd_transfer("base", _block(51381241, [_log()]), _log())
    assert row is not None
    assert row["blockNumber"] == 51381241
    assert row["tokenAddress"] == "0xtoken"
    assert row["from"] == "0x" + "aa" * 20
    assert row["to"] == "0x" + "bb" * 20
    assert row["value"] == "1000"
    assert row["logIndex"] == 7
    assert row["txHash"] == ""
    assert row["blockTimestamp"].startswith("2026-09-16T")
    assert set(row) == {
        "blockNumber",
        "txHash",
        "from",
        "to",
        "tokenAddress",
        "value",
        "blockTimestamp",
        "logIndex",
    }


def test_map_sqd_transfer_skips_non_transfer() -> None:
    assert map_sqd_transfer("base", _block(1, []), _log(topic0="0xdeadbeef")) is None
    # ERC-721-style 4-topic Transfer is not a fungible transfer.
    erc721 = _log(
        topics=[ERC20_TRANSFER_TOPIC, "0x" + _PAD + "aa" * 20, "0x" + _PAD + "bb" * 20, "0x" + _PAD + "cc" * 20]
    )
    assert map_sqd_transfer("base", _block(1, []), erc721) is None
    assert map_sqd_transfer("base", _block(1, []), {"topics": []}) is None


def test_map_sqd_transfer_big_value_exact() -> None:
    big = 2**80
    row = map_sqd_transfer("base", _block(1, []), _log(data="0x" + format(big, "064x")))
    assert row is not None
    assert row["value"] == str(big)  # > 2**63 stays an exact decimal string


def test_map_sqd_transfer_empty_data() -> None:
    row = map_sqd_transfer("base", _block(1, []), _log(data="0x"))
    assert row is not None and row["value"] == "0"


# ── Range chunking (fake portal records windows) ──────────────────────


class FakePortal:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, int]] = []

    def available(self) -> bool:
        return True

    def stream(self, dataset: str, from_block: int, to_block: int, **kw: Any) -> list[dict[str, Any]]:
        self.calls.append((dataset, from_block, to_block))
        assert kw["selectors"] == LOG_SELECTORS
        return [_block(bn, [_log()]) for bn in range(from_block, to_block + 1)]


def test_read_transfers_chunks_windows() -> None:
    portal = FakePortal()
    src = SqdIngestSource(portal=portal, window=2)
    rows = list(src.read_transfers("base", 1, 5))
    assert portal.calls == [("base-mainnet", 1, 2), ("base-mainnet", 3, 4), ("base-mainnet", 5, 5)]
    assert len(rows) == 5  # one transfer log per block (1..5)


def test_read_transfers_unknown_chain_yields_nothing() -> None:
    portal = FakePortal()
    assert list(SqdIngestSource(portal=portal).read_transfers("solana", 1, 5)) == []
    assert portal.calls == []


# ── Fill seam (fake source/writer) ────────────────────────────────────


class FakeSource:
    def __init__(self, n: int = 5, *, available: bool = True) -> None:
        self.n, self._available, self.calls = n, available, []

    def available(self) -> bool:
        return self._available

    def read_transfers(self, chain: str, from_block: int, to_block: int) -> Any:
        self.calls.append((chain, from_block, to_block))
        yield from ({"blockNumber": i} for i in range(self.n))


class FakeWriter:
    def __init__(self) -> None:
        self.batches: list[tuple[str, list[dict[str, Any]]]] = []

    def write_transfers(self, chain: str, events: list[dict[str, Any]]) -> int:
        self.batches.append((chain, list(events)))
        return len(events)


def test_dry_run_counts_without_writing() -> None:
    src, wh = FakeSource(n=4), FakeWriter()
    assert fill_chain_from_sqd(SqdFillRequest("base", 1, 10, dry_run=True), source=src, writer=wh) == {"rows": 4}
    assert src.calls == [("base", 1, 10)]
    assert wh.batches == []


def test_real_run_writes_batches() -> None:
    src, wh = FakeSource(n=5), FakeWriter()
    assert fill_chain_from_sqd(SqdFillRequest("base", 1, 10, batch_rows=2), source=src, writer=wh) == {"rows": 5}
    assert [len(b) for _, b in wh.batches] == [2, 2, 1]


def test_unavailable_source_returns_zero() -> None:
    assert fill_chain_from_sqd(
        SqdFillRequest("base", 1, 10), source=FakeSource(available=False), writer=FakeWriter()
    ) == {"rows": 0}


def test_unsupported_chain_and_range_raise() -> None:
    with pytest.raises(ValueError, match="unsupported sqd chain"):
        fill_chain_from_sqd(SqdFillRequest("solana", 1, 10), source=FakeSource(), writer=FakeWriter())
    with pytest.raises(ValueError, match="to_block is before"):
        fill_chain_from_sqd(SqdFillRequest("base", 10, 1), source=FakeSource(), writer=FakeWriter())


def test_map_sqd_transfer_joins_real_tx_hash() -> None:
    """The transactions selector supplies hashes; joined by transactionIndex."""
    block = _block(51381241, [_log()])
    block["transactions"] = [
        {"hash": "0xabc123", "transactionIndex": 0},
        {"hash": "0xdef456", "transactionIndex": 1},
    ]
    row = map_sqd_transfer("base", block, _log(), {0: "0xabc123", 1: "0xdef456"})
    assert row is not None and row["txHash"] == "0xdef456"


def test_map_sqd_transfer_missing_tx_hash_falls_back_empty() -> None:
    row = map_sqd_transfer("base", _block(1, [_log()]), _log(), {})
    assert row is not None and row["txHash"] == ""
