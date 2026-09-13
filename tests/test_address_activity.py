"""
Tests for the address_activity keystone table (wallet-tracing foundation).

Covers:
  1. parse_address_activity — one row per involved address from a
     getTransaction (jsonParsed) payload (signers, writable accounts,
     token accounts, read-only accounts) + signer derivation for the
     legacy plain-string account-key form.
  2. ClickHouse storage — ensure + insert for wallet_memory.address_activity.
  3. SolanaStrategy integration — the sample parsed tx is ingested into
     the strategy buffer and drained into address_activity.

Fixtures mirror real getTransaction (jsonParsed) responses (a token swap
shape with CPI inner instructions), reusing the constants style of
test_solana_traces.py.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from lite_indexer.storage.address_activity import (
    CH_ADDRESS_ACTIVITY_TABLE,
    ensure_address_activity_table,
    parse_address_activity,
    write_address_activity,
)
from lite_indexer.strategies.solana import SolanaStrategy

SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
COMPUTE_BUDGET = "ComputeBudget111111111111111111111111111111"

FEE_PAYER = "9A8ePyR48XkK8tVc4bjW9RQBq4WFc3jHs7GRoUjVU79C"
TRADER = "yyYx8k8a7Mz5zjZyvttXR984L86eG1KajSkBESzf7c1"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
ATA_IN = "8Y8qK5LgG7vYHmNpQwY1KkXy4wNwQV6bZkLJqE8rHjA2"
ATA_OUT = "3v2xKb9zTqRcVdEfGhIjKlMnOpQrStUvWxYzAbCdEfGh"

SIG = "5mLtwuNxKqdJvs8g3PDDjhLkCLhWNrPixfj7Ry2GXh7qmP9i1n3q8hBTmT6ZC89VRMzLpS9bwnB5N9yT2DiQaGAc"

BLOCK_TIME = 1_785_801_699
SLOT = 437_068_921
TS = datetime(2026, 8, 4, 0, 1, 39, tzinfo=UTC)


def _ix(program_id: str, parsed_type: str | None = None) -> dict[str, Any]:
    """Build a jsonParsed instruction dict."""
    ix: dict[str, Any] = {"programId": program_id}
    if parsed_type is not None:
        ix["parsed"] = {"type": parsed_type, "info": {"x": 1}}
    else:
        ix["data"] = "raw-payload"
        ix["accounts"] = []
    return ix


def _sample_tx(*, failed: bool = False) -> dict[str, Any]:
    """Build a getTransaction (jsonParsed) result with signers, writable
    accounts, token accounts, and a CPI inner instruction."""
    account_keys = [
        {"pubkey": FEE_PAYER, "signer": True, "writable": True},
        {"pubkey": TRADER, "signer": True, "writable": True},
        {"pubkey": ATA_IN, "signer": False, "writable": True},
        {"pubkey": ATA_OUT, "signer": False, "writable": True},
        {"pubkey": USDC_MINT, "signer": False, "writable": False},
        {"pubkey": TOKEN_PROGRAM, "signer": False, "writable": False},
        {"pubkey": SYSTEM_PROGRAM, "signer": False, "writable": False},
        {"pubkey": COMPUTE_BUDGET, "signer": False, "writable": False},
    ]
    return {
        "slot": SLOT,
        "blockTime": BLOCK_TIME,
        "version": 0,
        "meta": {
            "err": {"InstructionError": [0, "custom"]} if failed else None,
            "fee": 10_000,
            "innerInstructions": [{"index": 1, "instructions": [_ix(TOKEN_PROGRAM, "transferChecked")]}],
        },
        "transaction": {
            "signatures": [SIG],
            "message": {
                "header": {
                    "numRequiredSignatures": 2,
                    "numReadonlySignedAccounts": 0,
                    "numReadonlyUnsignedAccounts": 4,
                },
                "accountKeys": account_keys,
                "instructions": [
                    _ix(COMPUTE_BUDGET, None),
                    _ix(TOKEN_PROGRAM, "transferChecked"),
                    _ix(SYSTEM_PROGRAM, "transfer"),
                ],
            },
        },
    }


# ═══════════════════════════════════════════════════════════════════════
# parse_address_activity — parser core
# ═══════════════════════════════════════════════════════════════════════


class TestParseAddressActivity:
    def test_one_row_per_involved_address(self):
        rows = parse_address_activity(_sample_tx())
        # 8 account keys (signers + writable + token + programs) → 8 rows.
        assert len(rows) == 8
        addresses = [r["address"] for r in rows]
        assert addresses == [
            FEE_PAYER,
            TRADER,
            ATA_IN,
            ATA_OUT,
            USDC_MINT,
            TOKEN_PROGRAM,
            SYSTEM_PROGRAM,
            COMPUTE_BUDGET,
        ]

    def test_common_fields_repeated_on_every_row(self):
        rows = parse_address_activity(_sample_tx())
        for row in rows:
            assert row["tx_signature"] == SIG
            assert row["slot"] == SLOT
            assert row["block_time"] == TS
            assert row["involved_accounts"] == [
                FEE_PAYER,
                TRADER,
                ATA_IN,
                ATA_OUT,
                USDC_MINT,
                TOKEN_PROGRAM,
                SYSTEM_PROGRAM,
                COMPUTE_BUDGET,
            ]

    def test_signer_flags(self):
        rows = parse_address_activity(_sample_tx())
        by_address = {r["address"]: r["is_signer"] for r in rows}
        assert by_address[FEE_PAYER] is True
        assert by_address[TRADER] is True
        assert by_address[ATA_IN] is False
        assert by_address[USDC_MINT] is False
        assert by_address[TOKEN_PROGRAM] is False

    def test_program_ids_dedup_including_inner(self):
        rows = parse_address_activity(_sample_tx())
        # ComputeBudget + tokenProgram (top-level AND inner) + systemProgram
        assert rows[0]["program_ids"] == [COMPUTE_BUDGET, TOKEN_PROGRAM, SYSTEM_PROGRAM]

    def test_failed_transaction_skipped(self):
        assert parse_address_activity(_sample_tx(failed=True)) == []

    def test_plain_string_keys_derive_signers_from_header(self):
        tx = _sample_tx()
        tx["transaction"]["message"]["accountKeys"] = [FEE_PAYER, TRADER, USDC_MINT]
        tx["transaction"]["message"]["header"] = {"numRequiredSignatures": 2}
        rows = parse_address_activity(tx)
        assert len(rows) == 3
        by_address = {r["address"]: r["is_signer"] for r in rows}
        assert by_address[FEE_PAYER] is True
        assert by_address[TRADER] is True
        assert by_address[USDC_MINT] is False

    def test_missing_message_or_signatures_returns_empty(self):
        assert parse_address_activity({}) == []
        assert parse_address_activity({"transaction": {}}) == []
        tx = _sample_tx()
        tx["transaction"]["message"] = {"accountKeys": []}
        assert parse_address_activity(tx) == []


# ═══════════════════════════════════════════════════════════════════════
# ClickHouse storage
# ═══════════════════════════════════════════════════════════════════════


class TestAddressActivityStore:
    def test_ensure_creates_table(self):
        client = MagicMock()
        assert ensure_address_activity_table(client) is True
        # ensure() executes exactly one statement (the CREATE); the pre-015
        # upgrade ALTERs live in ensure_address_activity_chain_column().
        ddl = client.execute.call_args.args[0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert CH_ADDRESS_ACTIVITY_TABLE in ddl
        assert "ReplacingMergeTree" in ddl
        assert "chain String DEFAULT 'solana'" in ddl
        assert "ORDER BY (address, tx_signature, chain)" in ddl

    def test_write_inserts_rows(self):
        client = MagicMock()
        rows = parse_address_activity(_sample_tx())
        assert write_address_activity(client, rows) == 8
        call = client.execute.call_args
        sql, params = call.args[0], call.args[1]
        assert f"INSERT INTO {CH_ADDRESS_ACTIVITY_TABLE}" in sql
        assert len(params) == 8
        # Row shape: address, tx_signature, slot, block_time, involved, programs, is_signer, ingested_at
        first = params[0]
        assert first[0] == FEE_PAYER
        assert first[1] == SIG
        assert first[2] == SLOT
        assert first[6] is True
        assert isinstance(first[7], datetime)

    def test_write_empty_returns_zero(self):
        assert write_address_activity(MagicMock(), []) == 0

    def test_write_failure_returns_zero(self):
        client = MagicMock()
        client.execute.side_effect = RuntimeError("connection refused")
        rows = parse_address_activity(_sample_tx())
        assert write_address_activity(client, rows) == 0


# ═══════════════════════════════════════════════════════════════════════
# SolanaStrategy integration — sample tx → buffered → drained into the table
# ═══════════════════════════════════════════════════════════════════════


class TestAddressActivityIngestion:
    def test_ingest_buffers_rows_for_sample_tx(self):
        strategy = SolanaStrategy()
        buffered = strategy._ingest_address_activity(_sample_tx())
        assert buffered == 8
        assert len(strategy._address_activity_rows) == 8
        assert strategy._address_activity_rows[0]["address"] == FEE_PAYER

    def test_drain_writes_rows_to_address_activity(self):
        strategy = SolanaStrategy()
        strategy._ingest_address_activity(_sample_tx())
        client = MagicMock()
        strategy._drain_address_activity_rows(client)
        assert strategy._address_activity_rows == []  # drained
        ddl = client.execute.call_args_list[0].args[0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        insert_call = client.execute.call_args_list[1]
        sql, params = insert_call.args[0], insert_call.args[1]
        assert f"INSERT INTO {CH_ADDRESS_ACTIVITY_TABLE}" in sql
        assert len(params) == 8

    def test_drain_keeps_rows_on_failure(self):
        strategy = SolanaStrategy()
        strategy._ingest_address_activity(_sample_tx())
        client = MagicMock()
        client.execute.side_effect = RuntimeError("connection refused")
        strategy._drain_address_activity_rows(client)
        # Rows requeued for the next write cycle (bounded by the cap).
        assert len(strategy._address_activity_rows) == 8

    def test_failed_tx_yields_no_rows(self):
        strategy = SolanaStrategy()
        assert strategy._ingest_address_activity(_sample_tx(failed=True)) == 0
        assert strategy._address_activity_rows == []


# ═══════════════════════════════════════════════════════════════════════
# Slot propagation — the getBlock/catch-up path carries no payload slot
# ═══════════════════════════════════════════════════════════════════════


class TestAddressActivitySlotPropagation:
    def test_parse_prefers_explicit_slot_over_payload(self):
        tx = _sample_tx()
        tx["slot"] = 1  # stale/bogus payload value
        rows = parse_address_activity(tx, SLOT)
        assert {row["slot"] for row in rows} == {SLOT}

    def test_ingest_uses_explicit_slot_when_payload_lacks_it(self):
        strategy = SolanaStrategy()
        tx = _sample_tx()
        tx.pop("slot")  # getBlock per-tx shape (no top-level slot)
        buffered = strategy._ingest_address_activity(tx, SLOT)
        assert buffered == 8
        assert {row["slot"] for row in strategy._address_activity_rows} == {SLOT}

    def test_blockwise_path_writes_the_real_block_slot(self):
        strategy = SolanaStrategy(
            tracked_tokens=[{"mint": USDC_MINT, "symbol": "USDC", "decimals": 6, "name": "USD Coin"}]
        )
        block_slot = 441_234_568  # divisible by the catch-up sampling stride (every 4th slot)
        tx = _sample_tx()
        tx.pop("slot")  # getBlock transactions carry no top-level slot

        async def _fake_rpc(method: str, params: object = None, endpoint: object = None) -> dict[str, Any]:
            assert method == "getBlock"
            return {"result": {"blockTime": BLOCK_TIME, "transactions": [tx]}}

        strategy._rpc_call = _fake_rpc  # type: ignore[method-assign]
        asyncio.run(strategy._fetch_transfers_blockwise(block_slot, block_slot))

        assert strategy._address_activity_rows
        assert {row["slot"] for row in strategy._address_activity_rows} == {block_slot}
