"""Solana DEX swap parsing → wallet_memory.dex_events rows.

Parses swap instructions from the SAME getTransaction (jsonParsed) results
the SPL balance-change path already fetches (zero extra RPC). Detection is
program-id based: any top-level or inner instruction whose programId is a
known DEX program marks the transaction as containing a swap. Amounts come
from the transaction-level pre/post token balances (meta.preTokenBalances /
meta.postTokenBalances): the mint whose aggregate balance decreased is the
input leg, the mint that increased is the output leg.

Rows are emitted in the EVM dex_events event shape (blockNumber /
transactionHash / logIndex / token0 / token1 / amount0In / amount1Out / …)
so the shared ClickHouseWriter.write_dex_events can insert them into
wallet_memory.dex_events with chain='solana' unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .launch import (
    PUMP_SWAP_PROGRAM,
    RAYDIUM_AMM_V4_PROGRAM,
    RAYDIUM_CLMM_PROGRAM,
    RAYDIUM_CPMM_PROGRAM,
)

# Major Solana DEX programs → (dex label, version string).
# Raydium/PumpSwap ids are imported from launch.py (single source of truth);
# the rest are the canonical on-chain program addresses.
ORCA_WHIRLPOOL_PROGRAM = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
METEORA_DLMM_PROGRAM = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
METEORA_DAMM_V1_PROGRAM = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB"
METEORA_DAMM_V2_PROGRAM = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
JUPITER_V6_PROGRAM = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

DEX_PROGRAMS: dict[str, tuple[str, str]] = {
    RAYDIUM_AMM_V4_PROGRAM: ("raydium", "amm_v4"),
    RAYDIUM_CPMM_PROGRAM: ("raydium", "cpmm"),
    RAYDIUM_CLMM_PROGRAM: ("raydium", "clmm"),
    PUMP_SWAP_PROGRAM: ("pumpswap", "amm"),
    ORCA_WHIRLPOOL_PROGRAM: ("orca", "whirlpool"),
    METEORA_DLMM_PROGRAM: ("meteora", "dlmm"),
    METEORA_DAMM_V1_PROGRAM: ("meteora", "damm"),
    METEORA_DAMM_V2_PROGRAM: ("meteora", "damm_v2"),
    JUPITER_V6_PROGRAM: ("jupiter", "v6"),
}

# Best-effort pool-address account index per program (the account that is
# the pool/AMM state for a direct swap instruction). When the index does
# not exist or the program is not listed, contract_address stays ''.
_POOL_ACCOUNT_INDEX: dict[str, int] = {
    RAYDIUM_AMM_V4_PROGRAM: 1,  # amm id
    PUMP_SWAP_PROGRAM: 0,  # pool
    ORCA_WHIRLPOOL_PROGRAM: 0,  # whirlpool
}


def _program_id(ix: dict[str, Any]) -> str:
    pid = ix.get("programId")
    if isinstance(pid, dict):
        pid = pid.get("pubkey")
    return str(pid) if pid else ""


def _iter_instructions(tx_data: dict[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    """Yield (sequential index, instruction) over top-level + inner ixs."""
    out: list[tuple[int, dict[str, Any]]] = []
    message = (tx_data.get("transaction") or {}).get("message") or {}
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict):
            out.append((len(out), ix))
    for group in (tx_data.get("meta") or {}).get("innerInstructions") or []:
        for ix in group.get("instructions") or []:
            if isinstance(ix, dict):
                out.append((len(out), ix))
    return out


def _mint_deltas(meta: dict[str, Any]) -> dict[str, int]:
    """Aggregate raw token-balance deltas per mint across the whole tx."""
    deltas: dict[str, int] = {}

    def _apply(balances: list[dict[str, Any]] | None, sign: int) -> None:
        for bal in balances or []:
            mint = bal.get("mint") or ""
            if not mint:
                continue
            raw = (bal.get("uiTokenAmount") or {}).get("amount", "0")
            try:
                amount = int(raw)
            except (ValueError, TypeError):
                continue
            deltas[mint] = deltas.get(mint, 0) + sign * amount

    _apply(meta.get("preTokenBalances"), -1)
    _apply(meta.get("postTokenBalances"), +1)
    return {m: d for m, d in deltas.items() if d != 0}


def _resolve_dex(matched: list[str]) -> tuple[str, str]:
    """Pick the dex label/version from the matched program ids.

    Jupiter routes wrap an underlying DEX: when an underlying DEX program
    is present alongside Jupiter, attribute the swap to it; otherwise the
    dex is 'jupiter'.
    """
    non_jupiter = [p for p in matched if p != JUPITER_V6_PROGRAM]
    target = non_jupiter[0] if non_jupiter else (matched[0] if matched else "")
    return DEX_PROGRAMS.get(target, ("jupiter", "v6"))


def parse_solana_swap_events(
    tx_data: dict[str, Any],
    slot: int,
) -> list[dict[str, Any]]:
    """Parse DEX swap events from a Solana transaction.

    Args:
        tx_data: Parsed getTransaction result (jsonParsed encoding).
        slot: Slot number this transaction was confirmed in.

    Returns:
        List of dex_events-shaped swap rows (empty when the tx contains no
        instruction on a known DEX program or no in/out token flow).
    """
    meta = tx_data.get("meta") or {}
    if not meta or meta.get("err") is not None:
        return []

    instructions = _iter_instructions(tx_data)
    matched: list[str] = []
    matched_index = 0
    for idx, ix in instructions:
        pid = _program_id(ix)
        if pid in DEX_PROGRAMS:
            if not matched:
                matched_index = idx
            matched.append(pid)
    if not matched:
        return []

    deltas = _mint_deltas(meta)
    ins = [(m, d) for m, d in deltas.items() if d < 0]
    outs = [(m, d) for m, d in deltas.items() if d > 0]
    if not ins or not outs:
        return []
    # Largest flows win when fees/rent noise adds extra mints.
    in_mint, in_delta = max(ins, key=lambda x: -x[1])
    out_mint, out_delta = max(outs, key=lambda x: x[1])
    if in_mint == out_mint:
        return []

    dex, version = _resolve_dex(matched)

    transaction = tx_data.get("transaction") or {}
    signatures = transaction.get("signatures") or []
    tx_signature = signatures[0] if signatures else ""
    message = transaction.get("message") or {}
    account_keys = message.get("accountKeys") or []
    first = account_keys[0] if account_keys else None
    signer = first.get("pubkey") if isinstance(first, dict) else str(first or "")

    # Best-effort pool address from the first matched swap instruction.
    pool_address = ""
    for _, ix in instructions:
        if _program_id(ix) in matched:
            accounts = ix.get("accounts") or []
            pool_idx = _POOL_ACCOUNT_INDEX.get(_program_id(ix))
            if pool_idx is not None and pool_idx < len(accounts):
                pool_address = str(accounts[pool_idx])
            break

    block_time = tx_data.get("blockTime")
    if isinstance(block_time, (int, float)) and block_time > 0:
        ts = datetime.fromtimestamp(block_time, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    return [
        {
            "type": "swap",
            "chain": "solana",
            "blockNumber": slot,
            "blockTimestamp": ts,
            "transactionHash": tx_signature,
            "logIndex": matched_index,
            "pairAddress": pool_address,
            "dex": dex,
            "dex_type": version,
            "token0": in_mint,
            "token1": out_mint,
            "sender": signer,
            "recipient": signer,
            "amount0In": str(-in_delta),
            "amount1In": "0",
            "amount0Out": "0",
            "amount1Out": str(out_delta),
            "source": "lite_indexer",
        }
    ]
