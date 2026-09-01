"""Unit tests for Solana DEX swap parsing (lite_indexer/strategies/solana/swaps.py).

Pure-function tests with fixture transaction dicts — no live RPC. Covers
Raydium AMM v4 swaps, PumpSwap swaps, Jupiter attribution, and the
non-swap negative case.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.strategies.solana.swaps import (
    JUPITER_V6_PROGRAM,
    PUMP_SWAP_PROGRAM,
    RAYDIUM_AMM_V4_PROGRAM,
    parse_solana_swap_events,
)

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
MEMECOIN = "MemoCoinMint11111111111111111111111111111111"
SIGNATURE = "5" * 87
SIGNER = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"
SLOT = 300_000_000
BLOCK_TIME = 1_785_000_000


def _balance(mint: str, account_index: int, amount: int) -> dict[str, Any]:
    return {
        "mint": mint,
        "owner": SIGNER,
        "accountIndex": account_index,
        "uiTokenAmount": {"amount": str(amount), "uiAmount": amount / 1e6},
    }


def _tx(
    instructions: list[dict[str, Any]],
    pre_balances: list[dict[str, Any]],
    post_balances: list[dict[str, Any]],
    inner: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "err": None,
        "preTokenBalances": pre_balances,
        "postTokenBalances": post_balances,
    }
    if inner is not None:
        meta["innerInstructions"] = [{"index": 0, "instructions": inner}]
    return {
        "slot": SLOT,
        "blockTime": BLOCK_TIME,
        "transaction": {
            "signatures": [SIGNATURE],
            "message": {"accountKeys": [{"pubkey": SIGNER, "signer": True}], "instructions": instructions},
        },
        "meta": meta,
    }


def test_raydium_amm_v4_swap_parses() -> None:
    """A Raydium AMM v4 swap yields one dex_events row with correct mints/amounts."""
    tx = _tx(
        instructions=[{"programId": RAYDIUM_AMM_V4_PROGRAM, "accounts": ["poolX", "amm"]}],
        pre_balances=[
            _balance(WSOL, 0, 10_000_000_000),
            _balance(MEMECOIN, 1, 500_000_000),
        ],
        post_balances=[
            _balance(WSOL, 0, 9_000_000_000),
            _balance(MEMECOIN, 1, 1_500_000_000),
        ],
    )
    rows = parse_solana_swap_events(tx, SLOT)
    assert len(rows) == 1
    row = rows[0]
    assert row["chain"] == "solana"
    assert row["blockNumber"] == SLOT
    assert row["transactionHash"] == SIGNATURE
    assert row["dex"] == "raydium"
    assert row["token0"] == WSOL
    assert row["token1"] == MEMECOIN
    assert row["amount0In"] == "1000000000"
    assert row["amount1Out"] == "1000000000"
    assert row["amount1In"] == "0"
    assert row["amount0Out"] == "0"
    assert row["sender"] == SIGNER
    assert row["source"] == "lite_indexer"


def test_pumpswap_swap_parses() -> None:
    """A PumpSwap swap is detected with dex='pumpswap' and a pool address."""
    tx = _tx(
        instructions=[{"programId": PUMP_SWAP_PROGRAM, "accounts": ["poolAddr1", "vault"]}],
        pre_balances=[
            _balance(MEMECOIN, 0, 1_000_000),
            _balance(WSOL, 1, 0),
        ],
        post_balances=[
            _balance(MEMECOIN, 0, 0),
            _balance(WSOL, 1, 42_000_000),
        ],
    )
    rows = parse_solana_swap_events(tx, SLOT)
    assert len(rows) == 1
    row = rows[0]
    assert row["dex"] == "pumpswap"
    assert row["pairAddress"] == "poolAddr1"
    assert row["token0"] == MEMECOIN
    assert row["token1"] == WSOL
    assert row["amount0In"] == "1000000"
    assert row["amount1Out"] == "42000000"


def test_jupiter_swap_attributes_underlying_dex() -> None:
    """A Jupiter-routed swap whose inner instruction hits Raydium is
    attributed to the underlying DEX, not 'jupiter'."""
    tx = _tx(
        instructions=[{"programId": JUPITER_V6_PROGRAM, "accounts": []}],
        inner=[{"programId": RAYDIUM_AMM_V4_PROGRAM, "accounts": []}],
        pre_balances=[_balance(USDC, 0, 1_000_000), _balance(WSOL, 1, 0)],
        post_balances=[_balance(USDC, 0, 0), _balance(WSOL, 1, 3_000_000)],
    )
    rows = parse_solana_swap_events(tx, SLOT)
    assert len(rows) == 1
    assert rows[0]["dex"] == "raydium"


def test_non_swap_instruction_yields_no_rows() -> None:
    """A plain SPL transfer with no DEX program produces zero swap rows."""
    tx = _tx(
        instructions=[{"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "accounts": []}],
        pre_balances=[_balance(MEMECOIN, 0, 1_000_000)],
        post_balances=[_balance(MEMECOIN, 0, 0), _balance(MEMECOIN, 1, 1_000_000)],
    )
    assert parse_solana_swap_events(tx, SLOT) == []


def test_failed_tx_and_one_sided_flow_yield_no_rows() -> None:
    """Failed transactions and flows with only an increase (no input leg)
    produce zero swap rows."""
    failed = _tx([], [_balance(WSOL, 0, 1)], [_balance(WSOL, 0, 0)])
    failed["meta"]["err"] = {"InstructionError": [0, "custom"]}
    assert parse_solana_swap_events(failed, SLOT) == []

    one_sided = _tx(
        instructions=[{"programId": RAYDIUM_AMM_V4_PROGRAM, "accounts": []}],
        pre_balances=[_balance(WSOL, 0, 0)],
        post_balances=[_balance(WSOL, 0, 5)],
    )
    assert parse_solana_swap_events(one_sided, SLOT) == []
