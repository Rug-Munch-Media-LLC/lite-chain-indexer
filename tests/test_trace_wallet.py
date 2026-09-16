"""Tests for trace_wallet — multi-tier wallet history assembler."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

from lite_indexer.query.trace_wallet import trace_wallet

ADDR = "0xabcdef0000000000000000000000000000001234"


def _transfer(block: int, frm: str = ADDR, to: str = "0x" + "9" * 40) -> dict[str, Any]:
    return {
        "chain": "ethereum",
        "block_number": block,
        "tx_hash": f"0x{block:064x}",
        "from_address": frm,
        "to_address": to,
        "token_address": "0x" + "7" * 40,
        "value": "1000",
        "block_timestamp": datetime.now(UTC),
        "log_index": 1,
        "ingested_at": datetime.now(UTC),
    }


class FakeRouter:
    def __init__(self, rows: list[dict[str, Any]] | None = None, error: str | None = None) -> None:
        self.rows = rows or []
        self.error = error
        self.calls = 0

    async def get_transfers(
        self, chain: str, address: str, *, from_block: int, to_block: int, limit: int
    ) -> Any:
        self.calls += 1
        if self.error:
            raise RuntimeError(self.error)
        return self.rows[:limit]


class FakeEnvio:
    def __init__(
        self,
        transfers: list[dict[str, Any]] | None = None,
        traces: dict[int, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.transfers = transfers or []
        self.traces = traces or {}
        self.transfer_calls = 0
        self.trace_calls = 0

    def available(self) -> bool:
        return True

    def address_transfers(self, chain: str, address: str, frm: int, to: int, *, limit: int) -> Any:
        self.transfer_calls += 1
        return self.transfers[:limit]

    def block_traces_rows(self, chain: str, block: int) -> Any:
        self.trace_calls += 1
        return self.traces.get(block, [])


class FakeSQD:
    def __init__(self) -> None:
        self.solana_calls = 0
        self.tron_calls = 0
        self.hl_calls = 0

    def available(self) -> bool:
        return True

    def solana_owner_activity(self, owner: str, frm: int, to: int, *, limit: int) -> Any:
        self.solana_calls += 1
        return [
            {
                "chain": "solana",
                "slot": to - 1,
                "block_timestamp": datetime.now(UTC),
                "tx_hash": "",
                "transaction_index": 5,
                "program_id": "ProgA",
                "accounts": [owner],
                "account_count": 1,
                "data": "0x",
                "owner": owner,
                "ingested_at": datetime.now(UTC),
            }
        ]

    def tron_trc20_transfers(self, address: str, frm: int, to: int, *, limit: int) -> Any:
        self.tron_calls += 1
        return [_transfer(to - 2, frm="0x" + "1" * 40, to="0x" + "2" * 40) | {"chain": "tron"}]

    def tron_internal_transfers(self, address: str, frm: int, to: int, *, limit: int) -> Any:
        self.tron_calls += 1
        return [
            {
                "chain": "tron",
                "block_number": to - 1,
                "block_timestamp": datetime.now(UTC),
                "tx_hash": "",
                "trace_address": "1.0",
                "from_address": "0x" + "3" * 40,
                "to_address": "0x" + "4" * 40,
                "value_wei": "5",
                "call_type": "call",
                "gas_used": 0,
                "error": "",
                "input_method_id": "",
                "ingested_at": datetime.now(UTC),
            }
        ]

    def hyperliquid_owner_fills(self, user: str, frm: int, to: int, *, limit: int) -> Any:
        self.hl_calls += 1
        return [
            {
                "chain": "hyperliquid",
                "block_number": to - 1,
                "block_timestamp": datetime.now(UTC),
                "user": user,
                "coin": "BTC",
                "px": "1",
                "sz": "1",
                "side": "B",
                "direction": "Open Long",
                "fee": "0",
                "ingested_at": datetime.now(UTC),
            }
        ]


def test_evm_uses_router_and_skips_direct_envio() -> None:
    router = FakeRouter(rows=[_transfer(100), _transfer(101, frm="0x" + "5" * 40, to=ADDR)])
    envio = FakeEnvio(transfers=[_transfer(999)])
    out = asyncio.run(
        trace_wallet(
            "ethereum",
            ADDR,
            from_block=100,
            to_block=101,
            include_traces=False,
            router=router,
            envio=cast(Any, envio),
            sqd=cast(Any, FakeSQD()),
        )
    )
    assert out["counts"] == {"indexer": 2}
    assert router.calls == 1
    assert envio.transfer_calls == 0  # no duplicate direct fetch when federation served
    assert [r["block"] for r in out["rows"]] == [101, 100]
    assert all(r["kind"] == "transfer" and r["tier"] == "indexer" for r in out["rows"])


def test_evm_falls_back_to_direct_envio_without_router() -> None:
    envio = FakeEnvio(transfers=[_transfer(50)])
    out = asyncio.run(
        trace_wallet(
            "base",
            ADDR,
            from_block=50,
            to_block=50,
            include_traces=False,
            use_router=False,
            envio=cast(Any, envio),
            sqd=cast(Any, FakeSQD()),
        )
    )
    assert out["counts"] == {"envio": 1}
    assert envio.transfer_calls == 1


def test_evm_traces_window_filters_by_address() -> None:
    trace_match = {
        "chain": "base",
        "block_number": 200,
        "block_timestamp": datetime.now(UTC),
        "tx_hash": "0xt",
        "trace_address": "0.0",
        "from_address": ADDR,
        "to_address": "0x" + "8" * 40,
        "value_wei": "7",
        "call_type": "call",
        "gas_used": 1,
        "error": "",
        "input_method_id": "",
        "ingested_at": datetime.now(UTC),
    }
    trace_miss = dict(trace_match, from_address="0x" + "a" * 40, to_address="0x" + "b" * 40)
    envio = FakeEnvio(traces={199: [trace_miss], 200: [trace_match]})
    out = asyncio.run(
        trace_wallet(
            "base",
            ADDR,
            from_block=199,
            to_block=200,
            trace_window=2,
            use_router=False,
            envio=cast(Any, envio),
            sqd=cast(Any, FakeSQD()),
        )
    )
    kinds = [r["kind"] for r in out["rows"]]
    assert "internal_transfer" in kinds
    internals = [r for r in out["rows"] if r["kind"] == "internal_transfer"]
    assert len(internals) == 1
    assert internals[0]["block"] == 200
    assert envio.trace_calls == 2


def test_solana_uses_sqd_instructions() -> None:
    sqd = FakeSQD()
    out = asyncio.run(
        trace_wallet("solana", "OwnerAddr", from_block=1000, to_block=1001, sqd=cast(Any, sqd))
    )
    assert out["counts"] == {"sqd": 1}
    assert out["rows"][0]["kind"] == "instruction"
    assert sqd.solana_calls == 1


def test_tron_uses_sqd_trc20_and_internal() -> None:
    sqd = FakeSQD()
    out = asyncio.run(
        trace_wallet("tron", "TWallet", from_block=10, to_block=20, sqd=cast(Any, sqd))
    )
    kinds = sorted(r["kind"] for r in out["rows"])
    assert kinds == ["internal_transfer", "transfer"]
    assert sqd.tron_calls == 2


def test_hyperliquid_fills() -> None:
    sqd = FakeSQD()
    out = asyncio.run(
        trace_wallet("hyperliquid", "0xUser", from_block=5, to_block=6, sqd=cast(Any, sqd))
    )
    assert out["counts"] == {"sqd": 1}
    assert out["rows"][0]["kind"] == "fill"
    assert out["rows"][0]["coin"] == "BTC"


def test_unsupported_chain_notes() -> None:
    out = asyncio.run(trace_wallet("dogecoin", "DAddr"))
    assert out["rows"] == []
    assert any("not supported" in n for n in out["notes"])


def test_router_failure_is_noted_and_falls_back() -> None:
    router = FakeRouter(error="ch down")
    envio = FakeEnvio(transfers=[_transfer(9)])
    out = asyncio.run(
        trace_wallet(
            "ethereum",
            ADDR,
            from_block=9,
            to_block=9,
            include_traces=False,
            router=router,
            envio=cast(Any, envio),
            sqd=cast(Any, FakeSQD()),
        )
    )
    assert any("indexer federation failed" in n for n in out["notes"])
    assert out["counts"] == {"envio": 1}
