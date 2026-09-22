"""Tests for the router's SQD traces leg (internal_transfers, EVM)."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from lite_indexer.query.leg_request import LegRequest
from lite_indexer.query.router import QueryRouter


class FakeSQD:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[dict[str, Any]] = []

    def available(self) -> bool:
        return True

    def evm_traces(
        self, chain: str, from_block: int, to_block: int, *, address: Any = None, limit: int = 200
    ) -> Any:
        self.calls.append({"chain": chain, "from": from_block, "to": to_block, "address": address, "limit": limit})
        return self.rows


def _trace_row(block: int = 100) -> dict[str, Any]:
    return {
        "chain": "polygon",
        "block_number": block,
        "block_timestamp": None,
        "tx_hash": "",
        "trace_address": "0.0",
        "from_address": "0x" + "1" * 40,
        "to_address": "0x" + "2" * 40,
        "value_wei": "5",
        "call_type": "call",
        "gas_used": 21000,
        "error": "",
        "input_method_id": "",
        "ingested_at": None,
    }


def _router(sqd: Any) -> QueryRouter:
    return QueryRouter(sqd=cast(Any, sqd), on_demand_enabled=False)


def test_gated_to_internal_transfers() -> None:
    sqd = FakeSQD(rows=[_trace_row()])
    router = _router(sqd)
    out = asyncio.run(
        router._sqd_traces_route(LegRequest("evm_transfers", "polygon", from_block=1, to_block=2, filters=None, limit=10))
    )
    assert out == []
    assert sqd.calls == []


def test_requires_bounded_range_and_known_chain() -> None:
    sqd = FakeSQD(rows=[_trace_row()])
    router = _router(sqd)
    assert (
        asyncio.run(
            router._sqd_traces_route(LegRequest("internal_transfers", "polygon", from_block=None, to_block=2, filters=None, limit=10))
        )
        == []
    )
    assert (
        asyncio.run(
            router._sqd_traces_route(LegRequest("internal_transfers", "bsc", from_block=1, to_block=2, filters=None, limit=10))
        )
        == []
    )
    assert sqd.calls == []


def test_forwards_address_and_returns_filtered_rows() -> None:
    row = _trace_row()
    sqd = FakeSQD(rows=[row])
    router = _router(sqd)
    out = asyncio.run(
        router._sqd_traces_route(LegRequest(
            "internal_transfers",
            "polygon",
            from_block=10,
            to_block=20,
            filters={"from_address": row["from_address"]},
            limit=5,
        ))
    )
    assert len(out) == 1
    assert sqd.calls[0]["chain"] == "polygon"
    assert sqd.calls[0]["address"] == row["from_address"]
    assert sqd.calls[0]["limit"] == 5


def test_filter_mismatch_rejected_post_filter() -> None:
    sqd = FakeSQD(rows=[_trace_row()])
    router = _router(sqd)
    out = asyncio.run(
        router._sqd_traces_route(LegRequest(
            "internal_transfers",
            "polygon",
            from_block=10,
            to_block=20,
            filters={"from_address": "0x" + "9" * 40},
            limit=5,
        ))
    )
    assert out == []
