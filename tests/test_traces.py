"""
Tests for internal-transfer trace collection (lite_indexer.traces).

Covers:
  1. Parser tests — callTracer (geth) + trace_block (parity) → rows
  2. Filter semantics — DELEGATECALL/STATICCALL double-counting guard
  3. TraceCollector endpoint rotation + method fallback
  4. ClickHouse storage (ensure + insert)
  5. Indexer integration (traces_enabled wiring)
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from lite_indexer.traces.parsers import (
    ERC20_TRANSFER_SELECTOR,
    WETH_WITHDRAW_SELECTOR,
    extract_internal_transfers,
    input_method_id,
    parse_call_tracer,
    parse_parity_trace_block,
)

BLOCK = 19_700_000
TS = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)


# ── input_method_id ──────────────────────────────────────────────────


class TestInputMethodId:
    def test_weth_withdraw(self):
        assert input_method_id("0x2e1a7d4d0000000000000000000000000000000000000000000000000000000000000001") == (
            WETH_WITHDRAW_SELECTOR
        )

    def test_erc20_transfer(self):
        assert input_method_id("0xa9059cbb" + "00" * 62) == ERC20_TRANSFER_SELECTOR

    def test_empty_and_short(self):
        assert input_method_id("") == ""
        assert input_method_id("0x") == ""
        assert input_method_id("0x1234") == ""
        assert input_method_id(None) == ""


# ── callTracer (geth debug_traceBlockByNumber) ───────────────────────


def _call_frame(
    *,
    ftype: str = "CALL",
    value: str = "0x0",
    from_: str = "0x" + "aa" * 20,
    to: str = "0x" + "bb" * 20,
    input_: str | None = "0x2e1a7d4d0000000000000000000000000000000000000000000000000000000000000001",
    calls: list | None = None,
    error: str | None = None,
) -> dict:
    frame: dict = {
        "type": ftype,
        "from": from_,
        "to": to,
        "gas": "0x10000",
        "gasUsed": "0x5208",
        "input": input_,
    }
    if value != "0x0":
        frame["value"] = value
    if calls:
        frame["calls"] = calls
    if error:
        frame["error"] = error
    return frame


class TestParseCallTracer:
    def test_value_call_extracted(self):
        """A CALL with value>0 becomes an internal-transfer row."""
        result = [
            {
                "txHash": "0x" + "11" * 32,
                "result": _call_frame(value="0xde0b6b3a7640000"),  # 1 ETH
            }
        ]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        assert len(rows) == 1
        row = rows[0]
        assert row["chain"] == "ethereum"
        assert row["block_number"] == BLOCK
        assert row["block_timestamp"] == TS
        assert row["tx_hash"] == "0x" + "11" * 32
        assert row["trace_address"] == []
        assert row["from_address"] == "0x" + "aa" * 20
        assert row["to_address"] == "0x" + "bb" * 20
        assert row["value_wei"] == 10**18
        assert row["call_type"] == "call"
        assert row["gas_used"] == 0x5208
        assert row["input_method_id"] == WETH_WITHDRAW_SELECTOR
        assert row["error"] == ""

    def test_zero_value_call_skipped(self):
        """Zero-value CALLs are not internal transfers."""
        result = [{"txHash": "0x" + "11" * 32, "result": _call_frame(value="0x0")}]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        assert rows == []

    def test_nested_calls_trace_address(self):
        """Nested frames get a path; only value-moving ones are emitted."""
        inner = _call_frame(value="0xde0b6b3a7640000", to="0x" + "cc" * 20)
        outer = _call_frame(value="0x0", calls=[inner])
        result = [{"txHash": "0x" + "11" * 32, "result": outer}]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        assert len(rows) == 1
        assert rows[0]["trace_address"] == [0]
        assert rows[0]["to_address"] == "0x" + "cc" * 20

    def test_delegatecall_not_double_counted(self):
        """DELEGATECALL carrying the parent value must not emit a row.

        The value on a delegatecall frame is inherited context, not a new
        transfer — emitting it would double-count the parent CALL row.
        """
        delegate = _call_frame(
            ftype="DELEGATECALL",
            value="0xde0b6b3a7640000",
            from_="0x" + "aa" * 20,
            to="0x" + "dd" * 20,
        )
        parent = _call_frame(value="0xde0b6b3a7640000", calls=[delegate])
        result = [{"txHash": "0x" + "11" * 32, "result": parent}]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        # Only the parent CALL row; the DELEGATECALL is skipped.
        assert len(rows) == 1
        assert rows[0]["call_type"] == "call"

    def test_create_with_value(self):
        """CREATE with value funds a new contract."""
        frame = _call_frame(ftype="CREATE", value="0xde0b6b3a7640000", to="0x" + "ee" * 20)
        result = [{"txHash": "0x" + "11" * 32, "result": frame}]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        assert len(rows) == 1
        assert rows[0]["call_type"] == "create"
        assert rows[0]["input_method_id"] == ""  # init code, not a method call

    def test_selfdestruct(self):
        """SELFDESTRUCT pays balance to the beneficiary."""
        frame = _call_frame(
            ftype="SELFDESTRUCT",
            value="0x56bc75e2d63100000",  # 100 ETH
            from_="0x" + "aa" * 20,
            to="0x" + "ff" * 20,
        )
        result = [{"txHash": "0x" + "11" * 32, "result": frame}]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        assert len(rows) == 1
        assert rows[0]["call_type"] == "selfdestruct"
        assert rows[0]["value_wei"] == 100 * 10**18

    def test_error_kept(self):
        """Reverted frames still emit a row with the error populated."""
        frame = _call_frame(value="0xde0b6b3a7640000", error="execution reverted")
        result = [{"txHash": "0x" + "11" * 32, "result": frame}]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        assert len(rows) == 1
        assert rows[0]["error"] == "execution reverted"

    def test_missing_tx_hash(self):
        """Older geth responses may omit txHash — rows still parse."""
        result = [{"result": _call_frame(value="0xde0b6b3a7640000")}]
        rows = parse_call_tracer(result, "ethereum", BLOCK, TS)
        assert len(rows) == 1
        assert rows[0]["tx_hash"] == ""


# ── trace_block (parity / erigon) ────────────────────────────────────


def _parity_trace(
    *,
    ttype: str = "call",
    call_type: str = "call",
    value: str = "0x0",
    from_: str = "0x" + "aa" * 20,
    to: str = "0x" + "bb" * 20,
    trace_address: list | None = None,
    tx_hash: str = "0x" + "11" * 32,
    created: str | None = None,
    suicide_address: str | None = None,
    refund: str | None = None,
    balance: str | None = None,
) -> dict:
    action: dict = {"from": from_}
    if ttype == "call":
        action.update({"callType": call_type, "to": to, "value": value, "gas": "0x10000", "input": "0x"})
    elif ttype == "create":
        action.update({"value": value, "gas": "0x10000", "init": "0x"})
    elif ttype == "suicide":
        action.update({"address": suicide_address or from_, "balance": balance or "0x0", "refundAddress": refund or to})
    trace: dict = {
        "type": ttype,
        "action": action,
        "result": {"gasUsed": "0x5208", "output": "0x"},
        "traceAddress": trace_address or [],
        "subtraces": 0,
        "transactionHash": tx_hash,
        "transactionPosition": 0,
        "blockNumber": BLOCK,
    }
    if created:
        trace["result"]["address"] = created
    return trace


class TestParseParityTraceBlock:
    def test_call_with_value(self):
        rows = parse_parity_trace_block(
            [_parity_trace(value="0xde0b6b3a7640000")],
            "ethereum",
            BLOCK,
            TS,
        )
        assert len(rows) == 1
        assert rows[0]["value_wei"] == 10**18
        assert rows[0]["call_type"] == "call"
        assert rows[0]["tx_hash"] == "0x" + "11" * 32

    def test_delegatecall_skipped(self):
        rows = parse_parity_trace_block(
            [_parity_trace(call_type="delegatecall", value="0xde0b6b3a7640000")],
            "ethereum",
            BLOCK,
            TS,
        )
        assert rows == []

    def test_staticcall_skipped(self):
        rows = parse_parity_trace_block(
            [_parity_trace(call_type="staticcall", value="0xde0b6b3a7640000")],
            "ethereum",
            BLOCK,
            TS,
        )
        assert rows == []

    def test_create_with_value(self):
        rows = parse_parity_trace_block(
            [
                _parity_trace(
                    ttype="create",
                    value="0xde0b6b3a7640000",
                    created="0x" + "cc" * 20,
                )
            ],
            "ethereum",
            BLOCK,
            TS,
        )
        assert len(rows) == 1
        assert rows[0]["call_type"] == "create"
        assert rows[0]["to_address"] == "0x" + "cc" * 20

    def test_suicide(self):
        rows = parse_parity_trace_block(
            [
                _parity_trace(
                    ttype="suicide",
                    suicide_address="0x" + "aa" * 20,
                    refund="0x" + "ff" * 20,
                    balance="0x56bc75e2d63100000",  # 100 ETH
                )
            ],
            "ethereum",
            BLOCK,
            TS,
        )
        assert len(rows) == 1
        assert rows[0]["call_type"] == "selfdestruct"
        assert rows[0]["from_address"] == "0x" + "aa" * 20
        assert rows[0]["to_address"] == "0x" + "ff" * 20
        assert rows[0]["value_wei"] == 100 * 10**18

    def test_reward_skipped(self):
        """Block rewards are consensus-level, not internal transfers."""
        trace = _parity_trace()
        trace["type"] = "reward"
        trace["action"] = {"author": "0x" + "aa" * 20, "rewardType": "block", "value": "0x4563918244f40000"}
        rows = parse_parity_trace_block([trace], "ethereum", BLOCK, TS)
        assert rows == []

    def test_trace_address_preserved(self):
        rows = parse_parity_trace_block(
            [_parity_trace(value="0xde0b6b3a7640000", trace_address=[0, 1])],
            "ethereum",
            BLOCK,
            TS,
        )
        assert rows[0]["trace_address"] == [0, 1]

    def test_keeps_error(self):
        trace = _parity_trace(value="0xde0b6b3a7640000")
        trace["error"] = "reverted"
        rows = parse_parity_trace_block([trace], "ethereum", BLOCK, TS)
        assert rows[0]["error"] == "reverted"


# ── extract_internal_transfers (format autodetect) ───────────────────


class TestExtractInternalTransfers:
    def test_detects_parity_format(self):
        rows = extract_internal_transfers(
            [_parity_trace(value="0xde0b6b3a7640000")],
            "ethereum",
            BLOCK,
            TS,
        )
        assert len(rows) == 1
        assert rows[0]["call_type"] == "call"

    def test_detects_calltracer_format(self):
        result = [{"txHash": "0x" + "11" * 32, "result": _call_frame(value="0xde0b6b3a7640000")}]
        rows = extract_internal_transfers(result, "ethereum", BLOCK, TS)
        assert len(rows) == 1

    def test_empty(self):
        assert extract_internal_transfers([], "ethereum", BLOCK, TS) == []


# ── TraceCollector ───────────────────────────────────────────────────


class TestTraceCollector:
    @pytest.mark.asyncio
    async def test_collect_block_uses_calltracer(self, monkeypatch):
        from lite_indexer.traces.trace_collector import TraceCollector

        rpc = MagicMock()
        rpc.call = AsyncMock(
            return_value={
                "id": 1,
                "result": [
                    {
                        "txHash": "0x" + "11" * 32,
                        "result": _call_frame(value="0xde0b6b3a7640000"),
                    }
                ],
            }
        )
        rpc.eth_get_block_by_number = AsyncMock(return_value={"timestamp": "0x66c07c00"})
        collector = TraceCollector(
            chain="ethereum",
            endpoints=["https://eth.drpc.org"],
            rpc=rpc,
        )
        rows = await collector.collect_block_traces("ethereum", BLOCK)
        assert len(rows) == 1
        assert rows[0]["value_wei"] == 10**18
        # Method used was debug_traceBlockByNumber (callTracer).
        rpc.call.assert_awaited()
        assert rpc.call.await_args.args[1] == "debug_traceBlockByNumber"

    @pytest.mark.asyncio
    async def test_fallback_to_trace_block(self, monkeypatch):
        from lite_indexer.traces.trace_collector import TraceCollector

        rpc = MagicMock()
        # First call (debug_traceBlockByNumber) → method not supported.
        # Second call (trace_block) → parity result.
        rpc.call = AsyncMock(
            side_effect=[
                {"id": 1, "error": {"code": -32601, "message": "method not found"}},
                {
                    "id": 1,
                    "result": [_parity_trace(value="0xde0b6b3a7640000")],
                },
            ]
        )
        rpc.eth_get_block_by_number = AsyncMock(return_value={"timestamp": "0x66c07c00"})
        collector = TraceCollector(
            chain="ethereum",
            endpoints=["https://eth.drpc.org"],
            rpc=rpc,
        )
        rows = await collector.collect_block_traces("ethereum", BLOCK)
        assert len(rows) == 1
        assert rows[0]["call_type"] == "call"
        # Method cache should now prefer trace_block for this endpoint.
        assert collector._method_cache["https://eth.drpc.org"] == "trace_block"

    @pytest.mark.asyncio
    async def test_all_endpoints_fail_returns_empty(self):
        from lite_indexer.traces.trace_collector import TraceCollector

        rpc = MagicMock()
        rpc.call = AsyncMock(return_value=None)
        collector = TraceCollector(
            chain="ethereum",
            endpoints=["https://eth.drpc.org"],
            rpc=rpc,
        )
        rows = await collector.collect_block_traces("ethereum", BLOCK)
        assert rows == []

    @pytest.mark.asyncio
    async def test_no_endpoints_short_circuit(self):
        from lite_indexer.traces.trace_collector import TraceCollector

        collector = TraceCollector(chain="solana", endpoints=[], rpc=MagicMock())
        rows = await collector.collect_block_traces("solana", BLOCK)
        assert rows == []

    @pytest.mark.asyncio
    async def test_collect_range_concurrency(self):
        from lite_indexer.traces.trace_collector import TraceCollector

        rpc = MagicMock()
        rpc.call = AsyncMock(
            return_value={
                "id": 1,
                "result": [
                    {
                        "txHash": "0x" + "11" * 32,
                        "result": _call_frame(value="0xde0b6b3a7640000"),
                    }
                ],
            }
        )
        rpc.eth_get_block_by_number = AsyncMock(return_value={"timestamp": "0x66c07c00"})
        collector = TraceCollector(
            chain="ethereum",
            endpoints=["https://eth.drpc.org"],
            rpc=rpc,
        )
        rows = await collector.collect_range("ethereum", 1, 5, concurrency=2)
        assert len(rows) == 5

    @pytest.mark.asyncio
    async def test_collect_range_max_blocks(self):
        from lite_indexer.traces.trace_collector import TraceCollector

        rpc = MagicMock()
        # Non-empty result so _fetch_block returns truthy and the flow
        # reaches _block_timestamp; empty dict has no value transfers,
        # so extract_internal_transfers returns [].
        rpc.call = AsyncMock(return_value={"id": 1, "result": [{}]})
        rpc.eth_get_block_by_number = AsyncMock(return_value={"timestamp": "0x66c07c00"})
        collector = TraceCollector(
            chain="ethereum",
            endpoints=["https://eth.drpc.org"],
            rpc=rpc,
        )
        rows = await collector.collect_range("ethereum", 1, 100, max_blocks=3)
        assert rows == []  # no value calls in empty blocks
        # Only 3 blocks were swept, most-recent first (blocks 98, 99, 100).
        swept = {c.args[1] for c in rpc.eth_get_block_by_number.await_args_list}
        assert swept == {98, 99, 100}


# ── ClickHouse storage ───────────────────────────────────────────────


class TestTraceStore:
    @pytest.mark.asyncio
    async def test_ensure_table(self):
        from lite_indexer.storage.trace_store import ensure_internal_transfers_table

        client = MagicMock()
        client.execute = MagicMock(return_value=None)
        assert await ensure_internal_transfers_table(client) is True
        client.execute.assert_called_once()

    def test_write_rows(self):
        from lite_indexer.storage.trace_store import write_internal_transfers

        client = MagicMock()
        client.execute = MagicMock(return_value=None)
        rows = [
            {
                "chain": "ethereum",
                "block_number": BLOCK,
                "block_timestamp": TS,
                "tx_hash": "0x" + "11" * 32,
                "trace_address": [],
                "from_address": "0x" + "aa" * 20,
                "to_address": "0x" + "bb" * 20,
                "value_wei": 10**18,
                "call_type": "call",
                "gas_used": 0x5208,
                "error": "",
                "input_method_id": WETH_WITHDRAW_SELECTOR,
            }
        ]
        inserted = write_internal_transfers(client, "ethereum", rows)
        assert inserted == 1
        assert client.execute.called
        insert_sql = client.execute.call_args.args[0]
        assert "wallet_memory.internal_transfers" in insert_sql
        params = client.execute.call_args.args[1]
        assert params[0][7] == 10**18  # value_wei column

    def test_write_empty(self):
        from lite_indexer.storage.trace_store import write_internal_transfers

        client = MagicMock()
        assert write_internal_transfers(client, "ethereum", []) == 0
        client.execute.assert_not_called()

    def test_write_failure_returns_zero(self):
        from lite_indexer.storage.trace_store import write_internal_transfers

        client = MagicMock()
        client.execute = MagicMock(side_effect=RuntimeError("ch down"))
        rows = [
            {
                "chain": "ethereum",
                "block_number": BLOCK,
                "block_timestamp": TS,
                "tx_hash": "0x" + "11" * 32,
                "trace_address": [],
                "from_address": "0x" + "aa" * 20,
                "to_address": "0x" + "bb" * 20,
                "value_wei": 10**18,
                "call_type": "call",
                "gas_used": 0x5208,
                "error": "",
                "input_method_id": "",
            }
        ]
        assert write_internal_transfers(client, "ethereum", rows) == 0

    @pytest.mark.asyncio
    async def test_writer_delegation(self):
        """ClickHouseWriter.write_internal_transfers ensures + inserts."""
        from lite_indexer.storage.clickhouse import ClickHouseWriter

        writer = ClickHouseWriter()
        client = MagicMock()
        client.execute = MagicMock(return_value=None)
        writer._connect = MagicMock(return_value=client)  # type: ignore[method-assign]
        writer._trace_table_ready = False
        rows = [
            {
                "chain": "ethereum",
                "block_number": BLOCK,
                "block_timestamp": TS,
                "tx_hash": "0x" + "11" * 32,
                "trace_address": [],
                "from_address": "0x" + "aa" * 20,
                "to_address": "0x" + "bb" * 20,
                "value_wei": 10**18,
                "call_type": "call",
                "gas_used": 0x5208,
                "error": "",
                "input_method_id": WETH_WITHDRAW_SELECTOR,
            }
        ]
        inserted = await writer.write_internal_transfers("ethereum", rows)
        assert inserted == 1
        assert writer._trace_table_ready is True


# ── Indexer integration ──────────────────────────────────────────────


class TestIndexerTraceIntegration:
    @pytest.mark.asyncio
    async def test_traces_enabled_writes_ch(self, tmp_path, monkeypatch):
        from lite_indexer.indexer import LiteIndexer

        monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

        indexer = LiteIndexer(
            chain="ethereum",
            from_block=100,
            to_block=200,
            data_dir=tmp_path,
            traces_enabled=True,
        )
        strategy = indexer.strategy
        strategy.fetch_block_number = AsyncMock(return_value=500)
        strategy.fetch_transfers = AsyncMock(return_value=[])

        # No DEX / deployers — keep the test focused on traces.
        indexer.dex_enabled = False
        indexer.deployers_enabled = False

        trace_rows = [
            {
                "chain": "ethereum",
                "block_number": 200,
                "block_timestamp": "2026-08-01T00:00:00Z",
                "tx_hash": "0x" + "11" * 32,
                "trace_address": [],
                "from_address": "0x" + "aa" * 20,
                "to_address": "0x" + "bb" * 20,
                "value_wei": 10**18,
                "call_type": "call",
                "gas_used": 0x5208,
                "error": "",
                "input_method_id": WETH_WITHDRAW_SELECTOR,
            }
        ]

        async def fake_collect(from_block: int, to_block: int) -> list[dict]:
            return trace_rows

        indexer._collect_internal_transfers = AsyncMock(side_effect=fake_collect)

        ch = MagicMock()
        ch.write_internal_transfers = AsyncMock(return_value=1)
        ch.client.return_value = None  # skip reorg staging
        indexer._ch_writer = ch
        strategy.write_clickhouse = MagicMock(return_value=0)

        summary = await indexer._sync_loop_inner({"status": "ok", "errors": []}, 0.0)

        assert summary["trace_events_fetched"] == 1
        assert summary["traces_written_ch"] == 1
        ch.write_internal_transfers.assert_awaited_once_with("ethereum", trace_rows)

    @pytest.mark.asyncio
    async def test_traces_disabled_skips_collection(self, tmp_path, monkeypatch):
        from lite_indexer.indexer import LiteIndexer

        monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

        indexer = LiteIndexer(
            chain="ethereum",
            from_block=100,
            to_block=200,
            data_dir=tmp_path,
            traces_enabled=False,
        )
        indexer.dex_enabled = False
        indexer.deployers_enabled = False
        indexer._collect_internal_transfers = AsyncMock(return_value=[])

        ch = MagicMock()
        ch.write_internal_transfers = AsyncMock(return_value=0)
        ch.client.return_value = None
        indexer._ch_writer = ch
        indexer.strategy.fetch_block_number = AsyncMock(return_value=500)
        indexer.strategy.fetch_transfers = AsyncMock(return_value=[])

        summary = await indexer._sync_loop_inner({"status": "ok", "errors": []}, 0.0)

        indexer._collect_internal_transfers.assert_not_awaited()
        ch.write_internal_transfers.assert_not_awaited()
        assert "trace_events_fetched" not in summary
