"""
Tests for Solana trace-level data collection (lite_indexer.traces.solana).

Covers:
  1. extract_solana_transfers — all transfer types from a jsonParsed tx
  2. SolanaTraceCollector — RPC fetch + transfer graph building
  3. ClickHouse storage — ensure + insert for wallet_memory.solana_transfers
  4. SolanaStrategy integration — trace rows collected from the same
     getTransaction call and written alongside SPL balance changes

Fixtures mirror real getTransaction (jsonParsed) responses: a Jupiter
swap shape (AToken CPI createAccount, pump.fun token-2022 transferChecked,
wSOL transferChecked legs, closeAccount) plus simple SOL/stake txs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from lite_indexer.strategies.solana import SolanaStrategy
from lite_indexer.traces.solana import (
    ACCOUNT_CLOSE,
    ACCOUNT_CREATE,
    CPI_SOL,
    NATIVE_SOL,
    SOLANA_TRANSFERS_TABLE,
    SPL_TRANSFER,
    SPL_TRANSFER_CHECKED,
    STAKE_DELEGATE,
    SolanaTraceCollector,
    ensure_solana_transfers_table,
    extract_solana_transfers,
    write_solana_transfers,
)

SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
STAKE_PROGRAM = "Stake11111111111111111111111111111111111111"
COMPUTE_BUDGET = "ComputeBudget111111111111111111111111111111"

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
FEE_PAYER = "9A8ePyR48XkK8tVc4bjW9RQBq4WFc3jHs7GRoUjVU79C"
SIG = "5mLtwuNxKqdJvs8g3PDDjhLkCLhWNrPixfj7Ry2GXh7qmP9i1n3q8hBTmT6ZC89VRMzLpS9bwnB5N9yT2DiQaGAc"

BLOCK_TIME = 1_785_801_699
TS = datetime(2026, 8, 4, 1, 36, 39, tzinfo=UTC)


def _ix(
    program_id: str,
    parsed_type: str | None = None,
    info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a jsonParsed instruction dict."""
    ix: dict[str, Any] = {"programId": program_id}
    if parsed_type is not None:
        ix["parsed"] = {"type": parsed_type, "info": info or {}}
    else:
        ix["data"] = "raw-payload"
        ix["accounts"] = []
    return ix


def _base_tx(
    *,
    instructions: list[dict[str, Any]],
    inner: list[dict[str, Any]] | None = None,
    pre_balances: list[int] | None = None,
    post_balances: list[int] | None = None,
    extra_keys: list[str] | None = None,
    slot: int = 437_068_921,
) -> dict[str, Any]:
    """Build a getTransaction (jsonParsed) result dict."""
    account_keys = [
        {"pubkey": FEE_PAYER, "signer": True, "writable": True},
        {"pubkey": "yyYx8k8a7Mz5zjZyvttXR984L86eG1KajSkBESzf7c1", "signer": True, "writable": True},
    ]
    account_keys.extend({"pubkey": k, "signer": False, "writable": False} for k in (extra_keys or []))
    return {
        "slot": slot,
        "blockTime": BLOCK_TIME,
        "version": 0,
        "meta": {
            "err": None,
            "fee": 10_000,
            "preBalances": pre_balances or [1_000_000, 500_000_000],
            "postBalances": post_balances or [990_000, 500_000_000],
            "preTokenBalances": [],
            "postTokenBalances": [],
            "innerInstructions": inner or [],
        },
        "transaction": {
            "signatures": [SIG],
            "message": {"accountKeys": account_keys, "instructions": instructions},
        },
    }


# ═══════════════════════════════════════════════════════════════════════
# extract_solana_transfers — parser core
# ═══════════════════════════════════════════════════════════════════════


class TestExtractSolanaTransfers:
    def test_native_sol_transfer_outer(self):
        tx = _base_tx(
            instructions=[
                _ix(
                    SYSTEM_PROGRAM,
                    "transfer",
                    {"source": FEE_PAYER, "destination": "walletB", "lamports": 100_000_000},
                )
            ]
        )
        rows = extract_solana_transfers(tx)
        assert len(rows) == 1
        row = rows[0]
        assert row["transfer_type"] == NATIVE_SOL
        assert row["instruction_path"] == [0]
        assert row["from_address"] == FEE_PAYER
        assert row["to_address"] == "walletB"
        assert row["amount"] == 100_000_000
        assert row["mint"] == ""
        assert row["decimals"] == 9
        assert row["program_id"] == "systemProgram"
        assert row["fee_payer"] == FEE_PAYER
        assert row["signature"] == SIG
        assert row["chain"] == "solana"
        assert row["slot"] == 437_068_921

    def test_cpi_sol_transfer_inner(self):
        tx = _base_tx(
            instructions=[_ix(COMPUTE_BUDGET, None)],
            inner=[
                {
                    "index": 0,
                    "instructions": [
                        _ix(SYSTEM_PROGRAM, "transfer", {"source": "prog", "destination": "walletC", "lamports": 5_000})
                    ],
                }
            ],
        )
        rows = extract_solana_transfers(tx)
        assert len(rows) == 1
        assert rows[0]["transfer_type"] == CPI_SOL
        assert rows[0]["instruction_path"] == [0, 0]
        assert rows[0]["amount"] == 5_000

    def test_spl_transfer(self):
        tx = _base_tx(
            instructions=[
                _ix(
                    TOKEN_PROGRAM,
                    "transfer",
                    {
                        "source": "tokenA",
                        "destination": "tokenB",
                        "mint": USDC_MINT,
                        "amount": "2500000",
                        "tokenAmount": {"amount": "2500000", "decimals": 6, "uiAmount": 2.5},
                    },
                )
            ]
        )
        rows = extract_solana_transfers(tx)
        assert len(rows) == 1
        row = rows[0]
        assert row["transfer_type"] == SPL_TRANSFER
        assert row["amount"] == 2_500_000
        assert row["mint"] == USDC_MINT
        assert row["decimals"] == 6
        assert row["program_id"] == "tokenProgram"

    def test_spl_transfer_checked_with_decimals(self):
        tx = _base_tx(
            instructions=[
                _ix(
                    TOKEN_PROGRAM,
                    "transferChecked",
                    {
                        "source": "tokenA",
                        "destination": "tokenB",
                        "mint": USDC_MINT,
                        "authority": FEE_PAYER,
                        "tokenAmount": {"amount": "1000000", "decimals": 6, "uiAmount": 1.0},
                    },
                )
            ]
        )
        rows = extract_solana_transfers(tx)
        assert len(rows) == 1
        assert rows[0]["transfer_type"] == SPL_TRANSFER_CHECKED
        assert rows[0]["amount"] == 1_000_000
        assert rows[0]["decimals"] == 6

    def test_token_2022_transfer_checked(self):
        tx = _base_tx(
            instructions=[
                _ix(
                    TOKEN_2022_PROGRAM,
                    "transferChecked",
                    {
                        "source": "tokenA",
                        "destination": "tokenB",
                        "mint": "8Byg9wi43TNzgJpYta6UXxPz3LPe8v6ZwvmWWoWQpump",
                        "authority": FEE_PAYER,
                        "tokenAmount": {"amount": "12895059", "decimals": 6, "uiAmount": 12.895059},
                    },
                )
            ]
        )
        rows = extract_solana_transfers(tx)
        assert len(rows) == 1
        assert rows[0]["transfer_type"] == SPL_TRANSFER_CHECKED
        assert rows[0]["program_id"] == "token2022Program"

    def test_account_create_via_cpi(self):
        new_account = "FQ94owULfsCshEMWbCzAcmcUTUgcVRsumXA6UQTb46Uj"
        tx = _base_tx(
            instructions=[_ix("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", "createIdempotent", {})],
            inner=[
                {
                    "index": 0,
                    "instructions": [
                        _ix(
                            SYSTEM_PROGRAM,
                            "createAccount",
                            {
                                "source": FEE_PAYER,
                                "newAccount": new_account,
                                "lamports": 2_039_280,
                                "space": 165,
                                "owner": TOKEN_PROGRAM,
                            },
                        ),
                        _ix(TOKEN_PROGRAM, "initializeImmutableOwner", {}),
                        _ix(TOKEN_PROGRAM, "initializeAccount3", {"account": new_account, "mint": USDC_MINT}),
                    ],
                }
            ],
        )
        rows = extract_solana_transfers(tx)
        creates = [r for r in rows if r["transfer_type"] == ACCOUNT_CREATE]
        assert len(creates) == 1
        assert creates[0]["instruction_path"] == [0, 0]
        assert creates[0]["from_address"] == FEE_PAYER
        assert creates[0]["to_address"] == new_account
        assert creates[0]["amount"] == 2_039_280
        # initialize* state changes are NOT value movements
        assert len(rows) == 1

    def test_account_close_with_balance_delta(self):
        closed = "tokenAcctX"
        # pre: closed account holds rent-exempt 2_039_280 lamports; post: 0
        tx = _base_tx(
            instructions=[
                _ix(TOKEN_PROGRAM, "closeAccount", {"account": closed, "destination": FEE_PAYER, "owner": FEE_PAYER})
            ],
            extra_keys=[closed],
            pre_balances=[1_000_000, 500_000_000, 2_039_280],
            post_balances=[990_000, 500_000_000, 0],
        )
        rows = extract_solana_transfers(tx)
        closes = [r for r in rows if r["transfer_type"] == ACCOUNT_CLOSE]
        assert len(closes) == 1
        assert closes[0]["from_address"] == closed
        assert closes[0]["to_address"] == FEE_PAYER
        assert closes[0]["amount"] == 2_039_280

    def test_account_close_same_tx_create_and_close(self):
        # Net pre/post delta is 0 (created and closed in one swap), but the
        # rent funded at creation (2_039_280) is returned on close.
        closed = "FQ94owULfsCshEMWbCzAcmcUTUgcVRsumXA6UQTb46Uj"
        tx = _base_tx(
            instructions=[
                _ix(TOKEN_PROGRAM, "closeAccount", {"account": closed, "destination": FEE_PAYER, "owner": FEE_PAYER})
            ],
            inner=[
                {
                    "index": 0,
                    "instructions": [
                        _ix(
                            SYSTEM_PROGRAM,
                            "createAccount",
                            {
                                "source": FEE_PAYER,
                                "newAccount": closed,
                                "lamports": 2_039_280,
                                "space": 165,
                                "owner": TOKEN_PROGRAM,
                            },
                        )
                    ],
                }
            ],
            pre_balances=[1_000_000, 500_000_000, 0],
            post_balances=[990_000, 500_000_000, 0],
        )
        rows = extract_solana_transfers(tx)
        closes = [r for r in rows if r["transfer_type"] == ACCOUNT_CLOSE]
        assert len(closes) == 1
        assert closes[0]["amount"] == 2_039_280

    def test_stake_delegate(self):
        tx = _base_tx(
            instructions=[
                _ix(
                    STAKE_PROGRAM,
                    "delegate",
                    {"stakeAccount": "stakeAcct1", "votePubkey": "voteAcct1", "authorizedStaker": FEE_PAYER},
                )
            ]
        )
        rows = extract_solana_transfers(tx)
        delegates = [r for r in rows if r["transfer_type"] == STAKE_DELEGATE]
        assert len(delegates) == 1
        assert delegates[0]["from_address"] == "stakeAcct1"
        assert delegates[0]["to_address"] == "voteAcct1"
        assert delegates[0]["program_id"] == "stakeProgram"
        assert delegates[0]["amount"] == 0

    def test_raw_data_instruction_skipped(self):
        tx = _base_tx(instructions=[_ix("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", None)])
        assert extract_solana_transfers(tx) == []

    def test_zero_amount_transfers_skipped(self):
        tx = _base_tx(
            instructions=[
                _ix(SYSTEM_PROGRAM, "transfer", {"source": FEE_PAYER, "destination": "walletB", "lamports": 0}),
                _ix(TOKEN_PROGRAM, "transfer", {"source": "a", "destination": "b", "mint": USDC_MINT, "amount": "0"}),
            ]
        )
        assert extract_solana_transfers(tx) == []

    def test_slot_override_and_default(self):
        tx = _base_tx(
            instructions=[_ix(SYSTEM_PROGRAM, "transfer", {"source": FEE_PAYER, "destination": "w", "lamports": 1})]
        )
        assert extract_solana_transfers(tx)[0]["slot"] == 437_068_921
        assert extract_solana_transfers(tx, slot=123_456)[0]["slot"] == 123_456

    def test_empty_and_missing_meta(self):
        assert extract_solana_transfers({}) == []
        assert extract_solana_transfers({"meta": {}, "transaction": {"message": {}, "signatures": []}}) == []
        assert extract_solana_transfers(None) == []

    def test_jupiter_swap_shape_all_value_movements(self):
        """A swap-shaped tx with AToken CPI + wSOL legs + close."""
        new_account = "FQ94owULfsCshEMWbCzAcmcUTUgcVRsumXA6UQTb46Uj"
        tx = _base_tx(
            instructions=[
                _ix(COMPUTE_BUDGET, None),
                _ix("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", "createIdempotent", {}),
                _ix("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", None),
                _ix(
                    TOKEN_PROGRAM,
                    "closeAccount",
                    {"account": new_account, "destination": FEE_PAYER, "owner": FEE_PAYER},
                ),
            ],
            inner=[
                {
                    "index": 1,
                    "instructions": [
                        _ix(
                            SYSTEM_PROGRAM,
                            "createAccount",
                            {
                                "source": FEE_PAYER,
                                "newAccount": new_account,
                                "lamports": 2_039_280,
                                "space": 165,
                                "owner": TOKEN_PROGRAM,
                            },
                        ),
                        _ix(
                            TOKEN_PROGRAM,
                            "initializeAccount3",
                            {"account": new_account, "mint": "So11111111111111111111111111111111111111112"},
                        ),
                    ],
                },
                {
                    "index": 2,
                    "instructions": [
                        _ix(
                            TOKEN_PROGRAM,
                            "transferChecked",
                            {
                                "source": "srcWSol",
                                "destination": new_account,
                                "mint": "So11111111111111111111111111111111111111112",
                                "authority": "router",
                                "tokenAmount": {"amount": "98349", "decimals": 9, "uiAmount": 0.000098349},
                            },
                        )
                    ],
                },
            ],
            pre_balances=[1_000_000, 500_000_000, 0],
            post_balances=[990_000, 500_000_000, 0],
        )
        rows = extract_solana_transfers(tx)
        types = {r["transfer_type"] for r in rows}
        assert types == {ACCOUNT_CLOSE, ACCOUNT_CREATE, SPL_TRANSFER_CHECKED}
        paths = {tuple(r["instruction_path"]) for r in rows}
        assert (1, 0) in paths  # CPI createAccount
        assert (2, 0) in paths  # CPI wSOL transferChecked
        assert (3,) in paths  # outer closeAccount


# ═══════════════════════════════════════════════════════════════════════
# SolanaTraceCollector — RPC fetch + graph
# ═══════════════════════════════════════════════════════════════════════


class TestSolanaTraceCollector:
    async def test_collect_transaction_trace_passes_correct_params(self):
        collector = SolanaTraceCollector()
        collector._rpc_call = AsyncMock(return_value={"slot": 1, "transaction": {"message": {}}})  # type: ignore[method-assign]
        result = await collector.collect_transaction_trace(SIG)
        assert result == {"slot": 1, "transaction": {"message": {}}}
        args = collector._rpc_call.await_args
        assert args is not None
        assert args.args[0] == "getTransaction"
        params = args.args[1]
        assert params[0] == SIG
        assert params[1]["encoding"] == "jsonParsed"
        assert params[1]["maxSupportedTransactionVersion"] == 0

    async def test_collect_transaction_trace_none_on_failure(self):
        collector = SolanaTraceCollector()
        collector._rpc_call = AsyncMock(return_value=None)  # type: ignore[method-assign]
        assert await collector.collect_transaction_trace(SIG) is None

    async def test_extract_transfers_async_wrapper(self):
        collector = SolanaTraceCollector()
        tx = _base_tx(
            instructions=[_ix(SYSTEM_PROGRAM, "transfer", {"source": FEE_PAYER, "destination": "w", "lamports": 1})]
        )
        rows = await collector.extract_transfers(tx)
        assert len(rows) == 1
        assert rows[0]["transfer_type"] == NATIVE_SOL

    async def test_build_transfer_graph_aggregates_edges(self):
        collector = SolanaTraceCollector()
        tx = _base_tx(
            instructions=[
                _ix(SYSTEM_PROGRAM, "transfer", {"source": FEE_PAYER, "destination": "walletB", "lamports": 50_000}),
                _ix(
                    TOKEN_PROGRAM,
                    "transferChecked",
                    {
                        "source": "tA",
                        "destination": "tB",
                        "mint": USDC_MINT,
                        "authority": FEE_PAYER,
                        "tokenAmount": {"amount": "100", "decimals": 6},
                    },
                ),
                _ix(
                    TOKEN_PROGRAM,
                    "transferChecked",
                    {
                        "source": "tA",
                        "destination": "tB",
                        "mint": USDC_MINT,
                        "authority": FEE_PAYER,
                        "tokenAmount": {"amount": "200", "decimals": 6},
                    },
                ),
            ]
        )
        collector.collect_transaction_trace = AsyncMock(return_value=tx)  # type: ignore[method-assign]
        graph = await collector.build_transfer_graph(SIG)
        assert graph["signature"] == SIG
        assert graph["transfers"] == 3
        assert graph["chain"] == "solana"
        # Two legs of the same (from,to,mint,type) collapse into one edge
        usdc_edges = [e for e in graph["edges"] if e["token"] == USDC_MINT]
        assert len(usdc_edges) == 1
        assert usdc_edges[0]["amount"] == 300
        assert len(usdc_edges[0]["instruction_paths"]) == 2
        # SOL edge present, all addresses in nodes
        assert FEE_PAYER in graph["nodes"]
        assert "walletB" in graph["nodes"]

    async def test_build_transfer_graph_fetch_failure(self):
        collector = SolanaTraceCollector()
        collector.collect_transaction_trace = AsyncMock(return_value=None)  # type: ignore[method-assign]
        graph = await collector.build_transfer_graph(SIG)
        assert graph["error"] == "transaction_fetch_failed"


# ═══════════════════════════════════════════════════════════════════════
# ClickHouse storage
# ═══════════════════════════════════════════════════════════════════════


class TestSolanaTransfersStore:
    def _row(self, **overrides: Any) -> dict[str, Any]:
        row: dict[str, Any] = {
            "chain": "solana",
            "slot": 437_068_921,
            "block_timestamp": TS,
            "signature": SIG,
            "instruction_path": [0],
            "from_address": FEE_PAYER,
            "to_address": "walletB",
            "amount": 100_000_000,
            "mint": "",
            "decimals": 9,
            "program_id": "systemProgram",
            "transfer_type": NATIVE_SOL,
            "fee_payer": FEE_PAYER,
        }
        row.update(overrides)
        return row

    def test_ensure_creates_table(self):
        client = MagicMock()
        assert ensure_solana_transfers_table(client) is True
        ddl = client.execute.call_args.args[0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert SOLANA_TRANSFERS_TABLE in ddl

    def test_write_inserts_rows(self):
        client = MagicMock()
        rows = [self._row(), self._row(instruction_path=[1, 0], transfer_type=CPI_SOL, amount=5_000)]
        assert write_solana_transfers(client, rows) == 2
        call = client.execute.call_args
        sql, params = call.args[0], call.args[1]
        assert f"INSERT INTO {SOLANA_TRANSFERS_TABLE}" in sql
        assert len(params) == 2
        # amount/decimals/type preserved in order
        assert params[0][7] == 100_000_000
        assert params[0][8] == ""
        assert params[0][11] == NATIVE_SOL

    def test_write_empty_returns_zero(self):
        assert write_solana_transfers(MagicMock(), []) == 0

    def test_write_failure_returns_zero(self):
        client = MagicMock()
        client.execute.side_effect = RuntimeError("connection refused")
        assert write_solana_transfers(client, [self._row()]) == 0


# ═══════════════════════════════════════════════════════════════════════
# SolanaStrategy integration
# ═══════════════════════════════════════════════════════════════════════


class TestSolanaStrategyTraceIntegration:
    async def test_fetch_populates_trace_rows_from_same_rpc_call(self):
        strategy = SolanaStrategy()
        # Only USDC tracked so the tx is fetched once
        strategy.tracked_tokens = [{"mint": USDC_MINT, "symbol": "USDC", "decimals": 6, "name": "USD Coin"}]
        strategy._mint_set = {USDC_MINT}

        tx_data = _base_tx(
            instructions=[
                _ix(SYSTEM_PROGRAM, "transfer", {"source": FEE_PAYER, "destination": "walletB", "lamports": 50_000})
            ],
            pre_balances=[1_000_000, 500_000_000],
            post_balances=[990_000, 500_000_000],
        )

        async def fake_rpc(
            method: str, params: list[Any] | None = None, endpoint: str | None = None
        ) -> dict[str, Any] | None:
            if method == "getSignaturesForAddress":
                return {"result": [{"signature": SIG, "slot": 437_068_921, "err": None}]}
            if method == "getTransaction":
                return {"result": tx_data}
            if method == "getSlot":
                return {"result": 437_069_000}
            return {"result": None}

        strategy._rpc_call = fake_rpc  # type: ignore[method-assign]
        events, ok = await strategy._fetch_mint_transfers(USDC_MINT, 437_068_000, 437_069_000)
        assert ok is True
        assert len(strategy._trace_rows) == 1
        assert strategy._trace_rows[0]["transfer_type"] == NATIVE_SOL
        assert strategy._trace_rows[0]["signature"] == SIG
        # No SPL balance change in this tx (native SOL only) — events empty
        assert events == []

    async def test_write_clickhouse_writes_trace_rows(self):
        strategy = SolanaStrategy()
        strategy._trace_rows = [
            {
                "chain": "solana",
                "slot": 437_068_921,
                "block_timestamp": TS,
                "signature": SIG,
                "instruction_path": [0],
                "from_address": FEE_PAYER,
                "to_address": "walletB",
                "amount": 50_000,
                "mint": "",
                "decimals": 9,
                "program_id": "systemProgram",
                "transfer_type": NATIVE_SOL,
                "fee_payer": FEE_PAYER,
            }
        ]
        # One SPL event so the strategy proceeds past the empty guard
        events = [
            {
                "slot": 437_068_921,
                "blockTimestamp": "2026-08-04T01:36:39Z",
                "signature": SIG,
                "token_account": "tokenAcct",
                "mint": USDC_MINT,
                "owner": FEE_PAYER,
                "amount_pre": "0",
                "amount_post": "1000000",
                "change": "1000000",
                "decimals": 6,
                "symbol": "USDC",
                "token_name": "USD Coin",
            }
        ]
        client = MagicMock()
        ch_writer = MagicMock()
        ch_writer._connect.return_value = client

        written = strategy.write_clickhouse(events, ch_writer)
        assert written == 1  # SPL events count unchanged
        # Trace rows drained
        assert strategy._trace_rows == []
        sql_calls = [c.args[0] for c in client.execute.call_args_list]
        assert any("solana_spl_balance_changes" in s for s in sql_calls)
        assert any(SOLANA_TRANSFERS_TABLE in s for s in sql_calls)

    async def test_write_clickhouse_keeps_trace_rows_on_failure(self):
        strategy = SolanaStrategy()
        strategy._trace_rows = [
            {
                "chain": "solana",
                "slot": 1,
                "block_timestamp": TS,
                "signature": SIG,
                "instruction_path": [0],
                "from_address": FEE_PAYER,
                "to_address": "walletB",
                "amount": 1,
                "mint": "",
                "decimals": 9,
                "program_id": "systemProgram",
                "transfer_type": NATIVE_SOL,
                "fee_payer": FEE_PAYER,
            }
        ]
        client = MagicMock()
        client.execute.side_effect = RuntimeError("down")
        ch_writer = MagicMock()
        ch_writer._connect.return_value = client

        strategy.write_clickhouse([], ch_writer)
        # Trace rows retained for the next write cycle
        assert len(strategy._trace_rows) == 1
