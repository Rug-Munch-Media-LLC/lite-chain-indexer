"""
Tests for the Solana Helius adapter (lite_indexer.adapters.solana).

Covers:
  1. helius_rpc_url — resolves the Helius RPC from HELIUS_KEY
  2. SolanaHeliusAdapter.fetch_transfers — getSignaturesForAddress +
     getTransaction → solana_transfers rows
  3. SolanaHeliusAdapter.store_transfers — ensure + insert into
     wallet_memory.solana_transfers
  4. SolanaHeliusAdapter.fetch_and_store — end-to-end wrapper

Fixtures mirror real getTransaction (jsonParsed) responses (a simple SPL
transferChecked plus a native SOL transfer), matching the conventions in
tests/test_solana_traces.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

from lite_indexer.adapters.solana import (
    HELIUS_RPC_TEMPLATE,
    SolanaHeliusAdapter,
    helius_rpc_url,
)
from lite_indexer.traces.solana import (
    NATIVE_SOL,
    SPL_TRANSFER_CHECKED,
    ensure_solana_transfers_table,
    write_solana_transfers,
)

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SYSTEM_PROGRAM = "11111111111111111111111111111111"

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
FEE_PAYER = "9A8ePyR48XkK8tVc4bjW9RQBq4WFc3jHs7GRoUjVU79C"
SIG = "5mLtwuNxKqdJvs8g3PDDjhLkCLhWNrPixfj7Ry2GXh7qmP9i1n3q8hBTmT6ZC89VRMzLpS9bwnB5N9yT2DiQaGAc"

BLOCK_TIME = 1_785_801_699
TS = datetime(2026, 8, 4, 1, 36, 39, tzinfo=UTC)

TRACKED = [
    {"mint": USDC_MINT, "symbol": "USDC", "decimals": 6, "name": "USD Coin"},
]


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
    slot: int = 437_068_921,
) -> dict[str, Any]:
    """Build a getTransaction (jsonParsed) result dict."""
    account_keys = [
        {"pubkey": FEE_PAYER, "signer": True, "writable": True},
        {"pubkey": "yyYx8k8a7Mz5zjZyvttXR984L86eG1KajSkBESzf7c1", "signer": True, "writable": True},
    ]
    return {
        "slot": slot,
        "blockTime": BLOCK_TIME,
        "version": 0,
        "meta": {
            "err": None,
            "fee": 10_000,
            "preBalances": [1_000_000, 500_000_000],
            "postBalances": [990_000, 500_000_000],
            "preTokenBalances": [],
            "postTokenBalances": [],
            "innerInstructions": [],
        },
        "transaction": {
            "signatures": [SIG],
            "message": {"accountKeys": account_keys, "instructions": instructions},
        },
    }


def _spl_tx(slot: int = 437_068_921) -> dict[str, Any]:
    """A transferChecked SPL transaction (USDC)."""
    return _base_tx(
        slot=slot,
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
        ],
    )


def _sol_tx(slot: int = 437_068_921) -> dict[str, Any]:
    """A native SOL transfer transaction."""
    return _base_tx(
        slot=slot,
        instructions=[
            _ix(
                SYSTEM_PROGRAM,
                "transfer",
                {"source": FEE_PAYER, "destination": "walletB", "lamports": 100_000_000},
            )
        ],
    )


# ═══════════════════════════════════════════════════════════════════════
# helius_rpc_url
# ═══════════════════════════════════════════════════════════════════════


class TestHeliusRpcUrl:
    def test_resolves_when_key_set(self):
        with patch.dict("os.environ", {"HELIUS_KEY": "abc123"}, clear=False):
            url = helius_rpc_url()
            assert url == "https://rpc.helius.xyz/?api-key=abc123"

    def test_returns_none_when_key_unset(self):
        with patch.dict("os.environ", {}, clear=True):
            # Ensure HELIUS_KEY is absent.
            assert "HELIUS_KEY" not in __import__("os").environ
            assert helius_rpc_url() is None

    def test_template_constant(self):
        assert "${HELIUS_KEY}" in HELIUS_RPC_TEMPLATE


# ═══════════════════════════════════════════════════════════════════════
# SolanaHeliusAdapter.fetch_transfers
# ═══════════════════════════════════════════════════════════════════════


class TestFetchTransfers:
    def test_empty_when_from_gt_to(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED)
        assert asyncio_run(adapter.fetch_transfers(10, 5)) == []

    def test_fetches_and_parses_spl_transfer(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED, endpoint="https://rpc.helius.xyz/?api-key=test")
        tx = _spl_tx(slot=100)

        async def _fake_rpc_call(client, method, params=None):
            if method == "getSignaturesForAddress":
                return {"result": [{"signature": SIG, "slot": 100, "err": None}]}
            if method == "getTransaction":
                return {"result": tx}
            return None

        adapter._rpc_call = _fake_rpc_call  # type: ignore[method-assign]
        rows = asyncio_run(adapter.fetch_transfers(90, 110))
        assert len(rows) == 1
        row = rows[0]
        assert row["transfer_type"] == SPL_TRANSFER_CHECKED
        assert row["mint"] == USDC_MINT
        assert row["amount"] == 1_000_000
        assert row["decimals"] == 6
        assert row["slot"] == 100
        assert row["signature"] == SIG

    def test_filters_signatures_outside_slot_range(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED, endpoint="https://rpc.helius.xyz/?api-key=test")

        async def _fake_rpc_call(client, method, params=None):
            if method == "getSignaturesForAddress":
                # Two sigs: one in range (slot 100), one out of range (slot 500).
                return {
                    "result": [
                        {"signature": SIG, "slot": 100, "err": None},
                        {"signature": "other", "slot": 500, "err": None},
                    ]
                }
            if method == "getTransaction":
                return {"result": _spl_tx(slot=100)}
            return None

        adapter._rpc_call = _fake_rpc_call  # type: ignore[method-assign]
        rows = asyncio_run(adapter.fetch_transfers(90, 110))
        # Only the in-range signature is fetched → one row.
        assert len(rows) == 1

    def test_skips_failed_transactions(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED, endpoint="https://rpc.helius.xyz/?api-key=test")

        async def _fake_rpc_call(client, method, params=None):
            if method == "getSignaturesForAddress":
                return {"result": [{"signature": SIG, "slot": 100, "err": {"InstructionError": [0, 0]}}]}
            return None

        adapter._rpc_call = _fake_rpc_call  # type: ignore[method-assign]
        rows = asyncio_run(adapter.fetch_transfers(90, 110))
        assert rows == []

    def test_parses_native_sol_transfer(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED, endpoint="https://rpc.helius.xyz/?api-key=test")

        async def _fake_rpc_call(client, method, params=None):
            if method == "getSignaturesForAddress":
                return {"result": [{"signature": SIG, "slot": 100, "err": None}]}
            if method == "getTransaction":
                return {"result": _sol_tx(slot=100)}
            return None

        adapter._rpc_call = _fake_rpc_call  # type: ignore[method-assign]
        rows = asyncio_run(adapter.fetch_transfers(90, 110))
        assert len(rows) == 1
        assert rows[0]["transfer_type"] == NATIVE_SOL
        assert rows[0]["amount"] == 100_000_000

    def test_isolates_mint_failure(self):
        # One mint's RPC fails; the other still yields rows.
        tokens = [
            {"mint": USDC_MINT, "symbol": "USDC", "decimals": 6, "name": "USD Coin"},
            {"mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "symbol": "USDT", "decimals": 6, "name": "Tether"},
        ]
        adapter = SolanaHeliusAdapter(tracked_tokens=tokens, endpoint="https://rpc.helius.xyz/?api-key=test")

        async def _fake_rpc_call(client, method, params=None):
            if method == "getSignaturesForAddress":
                mint = params[0] if params else ""
                if mint == USDC_MINT:
                    return {"result": [{"signature": SIG, "slot": 100, "err": None}]}
                raise RuntimeError("boom")
            if method == "getTransaction":
                return {"result": _spl_tx(slot=100)}
            return None

        adapter._rpc_call = _fake_rpc_call  # type: ignore[method-assign]
        rows = asyncio_run(adapter.fetch_transfers(90, 110))
        assert len(rows) == 1
        assert rows[0]["mint"] == USDC_MINT


# ═══════════════════════════════════════════════════════════════════════
# SolanaHeliusAdapter.store_transfers
# ═══════════════════════════════════════════════════════════════════════


class TestStoreTransfers:
    def test_returns_zero_for_empty(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED)
        assert adapter.store_transfers(MagicMock(), []) == 0

    def test_ensures_table_and_writes(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED)
        client = MagicMock()
        rows = [{"transfer_type": SPL_TRANSFER_CHECKED, "mint": USDC_MINT, "amount": 1_000_000}]
        with (
            patch(
                "lite_indexer.adapters.solana.ensure_solana_transfers_table",
                return_value=True,
            ) as ensure_mock,
            patch(
                "lite_indexer.adapters.solana.write_solana_transfers",
                return_value=1,
            ) as write_mock,
        ):
            written = adapter.store_transfers(client, rows)
        ensure_mock.assert_called_once_with(client)
        write_mock.assert_called_once_with(client, rows)
        assert written == 1

    def test_writes_through_real_helpers(self):
        # Integration-style: exercise the real ensure + write helpers with a
        # fake ClickHouse client that records the INSERT.
        client = MagicMock()
        rows = [
            {
                "chain": "solana",
                "slot": 100,
                "block_timestamp": TS,
                "signature": SIG,
                "instruction_path": [0],
                "from_address": FEE_PAYER,
                "to_address": "walletB",
                "amount": 1_000_000,
                "mint": USDC_MINT,
                "decimals": 6,
                "program_id": "tokenProgram",
                "transfer_type": SPL_TRANSFER_CHECKED,
                "fee_payer": FEE_PAYER,
            }
        ]
        assert ensure_solana_transfers_table(client) is True
        assert write_solana_transfers(client, rows) == 1
        client.execute.assert_called()


# ═══════════════════════════════════════════════════════════════════════
# SolanaHeliusAdapter.fetch_and_store
# ═══════════════════════════════════════════════════════════════════════


class TestFetchAndStore:
    def test_fetch_and_store_writes_rows(self):
        adapter = SolanaHeliusAdapter(tracked_tokens=TRACKED, endpoint="https://rpc.helius.xyz/?api-key=test")
        client = MagicMock()

        async def _fake_rpc_call(client, method, params=None):
            if method == "getSignaturesForAddress":
                return {"result": [{"signature": SIG, "slot": 100, "err": None}]}
            if method == "getTransaction":
                return {"result": _spl_tx(slot=100)}
            return None

        adapter._rpc_call = _fake_rpc_call  # type: ignore[method-assign]
        with (
            patch(
                "lite_indexer.adapters.solana.ensure_solana_transfers_table",
                return_value=True,
            ),
            patch(
                "lite_indexer.adapters.solana.write_solana_transfers",
                return_value=1,
            ),
        ):
            written = asyncio_run(adapter.fetch_and_store(client, 90, 110))
        assert written == 1


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════


def asyncio_run(coro):
    """Run a coroutine to completion (pytest-asyncio may not be configured)."""
    import asyncio

    return asyncio.run(coro)
