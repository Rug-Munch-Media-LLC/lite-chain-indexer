"""Solana trace parsing — extract value transfers from parsed transactions.

Split out of the former monolithic ``traces/solana.py`` (939 lines).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lite_indexer.metrics import incr as metrics_incr

from .dex import _program_name
from .helpers import (
    ACCOUNT_CLOSE,
    ACCOUNT_CREATE,
    CPI_SOL,
    NATIVE_SOL,
    SPL_TRANSFER,
    SPL_TRANSFER_CHECKED,
    STAKE_DELEGATE,
    STAKE_PROGRAM,
    SYSTEM_PROGRAM,
    TOKEN_2022_PROGRAM,
    TOKEN_PROGRAM,
    _account_pubkey,
    _balance_delta,
    _to_int,
    _to_timestamp,
)
from .spl import spl_transfer_row_fields


def extract_solana_transfers(tx_data: dict[str, Any] | None, slot: int | None = None) -> list[dict[str, Any]]:
    """Extract ALL value transfers from a parsed Solana transaction.

    Scans the outer instruction tree and ``meta.innerInstructions`` (CPI
    tree) for value movements:

      * native SOL transfers (``systemProgram.transfer``)
      * SPL Token transfers (``tokenProgram.transfer`` / ``transferChecked``,
        including token-2022)
      * SOL moved through CPIs (inner systemProgram transfers)
      * token account creation / closing
      * stake delegations

    Rows follow the ``wallet_memory.solana_transfers`` schema: ``chain,
    slot, block_timestamp, signature, instruction_path, from_address,
    to_address, amount, mint, decimals, program_id, transfer_type,
    fee_payer``.

    Args:
        tx_data: Parsed ``getTransaction`` result (jsonParsed encoding).
        slot: Slot number (defaults to ``tx_data["slot"]`` when omitted).

    Returns:
        List of standardized transfer-trace row dicts (may be empty).
    """
    if not isinstance(tx_data, dict):
        return []

    meta = tx_data.get("meta")
    if not isinstance(meta, dict):
        return []
    transaction = tx_data.get("transaction")
    if not isinstance(transaction, dict):
        return []
    message = transaction.get("message")
    if not isinstance(message, dict):
        return []

    account_keys = message.get("accountKeys") or []
    signatures = transaction.get("signatures") or []
    signature = str(signatures[0]) if signatures else ""
    fee_payer = _account_pubkey(account_keys, 0)
    block_ts = _to_timestamp(tx_data.get("blockTime"))
    tx_slot = int(slot) if slot is not None else _to_int(tx_data.get("slot"), 0)

    pre_balances = meta.get("preBalances") or []
    post_balances = meta.get("postBalances") or []

    rows: list[dict[str, Any]] = []

    # First pass: find token accounts created inside this transaction
    # (systemProgram.createAccount / createAccountWithSeed, top-level or
    # CPI) and the lamports funded into them. Used to value account_close
    # rows when the account is created and closed in the same tx — the
    # net pre/post balance delta is 0 (money in, money out) but the rent
    # returned on close is a real SOL movement.
    created_lamports: dict[str, int] = {}
    for ix in _iter_all_instructions(message, meta):
        program_id = str(ix.get("programId") or "")
        parsed = ix.get("parsed")
        if program_id != SYSTEM_PROGRAM or not isinstance(parsed, dict):
            continue
        parsed_info = parsed.get("info")
        if not isinstance(parsed_info, dict):
            continue
        if parsed.get("type") in ("createAccount", "createAccountWithSeed"):
            lamports = _to_int(parsed_info.get("lamports"), 0)
            new_account = str(parsed_info.get("newAccount") or "")
            if lamports > 0 and new_account:
                created_lamports[new_account] = lamports

    # ── Outer instructions ────────────────────────────────────────────
    for outer_index, ix in enumerate(message.get("instructions") or []):
        if not isinstance(ix, dict):
            continue
        rows.extend(
            _extract_instruction_rows(
                ix,
                instruction_path=[outer_index],
                inner=False,
                account_keys=account_keys,
                pre_balances=pre_balances,
                post_balances=post_balances,
                created_lamports=created_lamports,
                chain="solana",
                slot=tx_slot,
                block_timestamp=block_ts,
                signature=signature,
                fee_payer=fee_payer,
            )
        )

    # ── CPI inner instructions ────────────────────────────────────────
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        outer_index = _to_int(group.get("index"), 0)
        for inner_index, ix in enumerate(group.get("instructions") or []):
            if not isinstance(ix, dict):
                continue
            rows.extend(
                _extract_instruction_rows(
                    ix,
                    instruction_path=[outer_index, inner_index],
                    inner=True,
                    account_keys=account_keys,
                    pre_balances=pre_balances,
                    post_balances=post_balances,
                    created_lamports=created_lamports,
                    chain="solana",
                    slot=tx_slot,
                    block_timestamp=block_ts,
                    signature=signature,
                    fee_payer=fee_payer,
                )
            )

    metrics_incr("solana_trace_rows_extracted", len(rows))
    return rows


def _iter_all_instructions(message: dict[str, Any], meta: dict[str, Any]) -> list[dict[str, Any]]:
    """Yield every instruction in a transaction (outer + CPI, flat).

    Args:
        message: The transaction's ``message`` dict.
        meta: The transaction's ``meta`` dict.

    Returns:
        Flat list of instruction dicts in tree order.
    """
    instructions: list[dict[str, Any]] = []
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict):
            instructions.append(ix)
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        for ix in group.get("instructions") or []:
            if isinstance(ix, dict):
                instructions.append(ix)
    return instructions


def _extract_instruction_rows(
    ix: dict[str, Any],
    *,
    instruction_path: list[int],
    inner: bool,
    account_keys: list[Any],
    pre_balances: list[Any],
    post_balances: list[Any],
    created_lamports: dict[str, int],
    chain: str,
    slot: int,
    block_timestamp: datetime | None,
    signature: str,
    fee_payer: str,
) -> list[dict[str, Any]]:
    """Emit transfer rows for one instruction (outer or CPI).

    Instructions without ``parsed`` data (raw byte payloads from DEX
    routers etc.) carry no structured transfer info and are skipped.

    Args:
        ix: Instruction dict from the parsed transaction.
        instruction_path: Path in the instruction tree ([outer] or
            [outer, inner]).
        inner: True for CPI instructions (affects SOL transfer typing).
        account_keys: Transaction account key list.
        pre_balances: ``meta.preBalances`` (for close-account amounts).
        post_balances: ``meta.postBalances``.
        created_lamports: Account -> lamports funded in this tx (from the
            systemProgram createAccount pass).
        chain: Chain name ('solana').
        slot: Transaction slot.
        block_timestamp: Transaction timestamp.
        signature: Transaction signature.
        fee_payer: Transaction fee payer (accountKeys[0]).

    Returns:
        Transfer rows for this instruction (0-2 usually, empty when the
        instruction is not a value movement).
    """
    program_id = str(ix.get("programId") or "")
    parsed = ix.get("parsed")
    if not isinstance(parsed, dict):
        return []
    ptype = parsed.get("type")
    parsed_info = parsed.get("info")
    if not isinstance(parsed_info, dict):
        parsed_info = {}

    def make_row(
        *,
        transfer_type: str,
        from_address: str,
        to_address: str,
        amount: int,
        mint: str = "",
        decimals: int = 9,
    ) -> dict[str, Any]:
        return {
            "chain": chain,
            "slot": slot,
            "block_timestamp": block_timestamp,
            "signature": signature,
            "instruction_path": list(instruction_path),
            "from_address": from_address,
            "to_address": to_address,
            "amount": amount,
            "mint": mint,
            "decimals": decimals,
            "program_id": _program_name(program_id),
            "transfer_type": transfer_type,
            "fee_payer": fee_payer,
        }

    # ── System program: native SOL movements ─────────────────────────
    if program_id == SYSTEM_PROGRAM:
        if ptype in ("transfer", "transferWithSeed"):
            amount = _to_int(parsed_info.get("lamports"), 0)
            if amount <= 0:
                return []
            return [
                make_row(
                    transfer_type=CPI_SOL if inner else NATIVE_SOL,
                    from_address=str(parsed_info.get("source") or ""),
                    to_address=str(parsed_info.get("destination") or ""),
                    amount=amount,
                )
            ]
        if ptype in ("createAccount", "createAccountWithSeed"):
            amount = _to_int(parsed_info.get("lamports"), 0)
            if amount <= 0:
                return []
            return [
                make_row(
                    transfer_type=ACCOUNT_CREATE,
                    from_address=str(parsed_info.get("source") or ""),
                    to_address=str(parsed_info.get("newAccount") or ""),
                    amount=amount,
                )
            ]
        return []

    # ── SPL token programs: token transfers + account state changes ──
    if program_id in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        if ptype in ("transfer", "transferChecked"):
            fields = spl_transfer_row_fields(parsed_info, checked=ptype == "transferChecked")
            if fields is None:
                return []
            return [
                make_row(
                    transfer_type=SPL_TRANSFER_CHECKED if ptype == "transferChecked" else SPL_TRANSFER,
                    from_address=str(parsed_info.get("source") or ""),
                    to_address=str(parsed_info.get("destination") or ""),
                    amount=fields["amount"],
                    mint=fields["mint"],
                    decimals=fields["decimals"],
                )
            ]
        if ptype == "closeAccount":
            account = str(parsed_info.get("account") or "")
            destination = str(parsed_info.get("destination") or "")
            amount = _balance_delta(pre_balances, post_balances, account, account_keys)
            if amount <= 0:
                # Account created AND closed in the same tx: the net
                # balance delta is 0, but the rent funded at creation is
                # returned on close — use the same-tx creation amount.
                amount = created_lamports.get(account, 0)
            return [
                make_row(
                    transfer_type=ACCOUNT_CLOSE,
                    from_address=account,
                    to_address=destination,
                    amount=amount,
                )
            ]
        # initializeAccount / initializeAccount3 / initializeImmutableOwner
        # etc. are account state changes — the rent SOL movement is
        # already captured by the systemProgram createAccount CPI.
        return []

    # ── Stake program: delegations ───────────────────────────────────
    if program_id == STAKE_PROGRAM and ptype == "delegate":
        return [
            make_row(
                transfer_type=STAKE_DELEGATE,
                from_address=str(parsed_info.get("stakeAccount") or parsed_info.get("stakePubkey") or ""),
                to_address=str(parsed_info.get("votePubkey") or ""),
                amount=0,
            )
        ]

    return []


__all__ = ["extract_solana_transfers"]
