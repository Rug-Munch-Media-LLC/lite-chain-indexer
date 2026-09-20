"""Solana readers for :class:`lite_indexer.query.sqd_source.SQDSource`.

Per-owner instruction history and token-balance diffs. Functions take the
portal client (``src``) so the facade keeps the public method names.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lite_indexer.query.sqd_common import ts

if TYPE_CHECKING:
    from lite_indexer.query.sqd_source import SQDSource


def solana_owner_activity(
    src: SQDSource,
    owner: str,
    from_slot: int,
    to_slot: int,
    *,
    limit: int = 500,
    max_batches: int = 48,
) -> list[dict[str, Any]]:
    """Instructions mentioning ``owner`` — wallet-scoped Solana history.

    Portal's ``mentionsAccount`` selector is the supported owner filter.
    ``preOwner``/``postOwner`` are **not** filterable (verified
    2026-09-16: unknown filter keys are dropped and the response
    degrades to worker-boundary blocks with no records).
    """
    fields = {
        "block": {"number": True, "timestamp": True},
        "instruction": {"programId": True, "accounts": True, "data": True, "transactionIndex": True},
    }
    blocks = src.stream(
        "solana-mainnet",
        from_slot,
        to_slot,
        fields=fields,
        selectors={"instructions": [{"mentionsAccount": [owner]}]},
        max_batches=max_batches,
    )
    rows: list[dict[str, Any]] = []
    for block in blocks:
        header = block.get("header", {})
        slot = int(header.get("number", 0))
        block_ts = ts(header.get("timestamp"))
        for rec in block.get("instructions", []) or []:
            accounts = rec.get("accounts") or []
            if owner not in accounts:
                continue
            rows.append(
                {
                    "chain": "solana",
                    "slot": slot,
                    "block_timestamp": block_ts,
                    "tx_hash": "",
                    "transaction_index": rec.get("transactionIndex"),
                    "program_id": rec.get("programId") or "",
                    "accounts": accounts,
                    "account_count": len(accounts),
                    "data": rec.get("data") or "",
                    "owner": owner,
                    "ingested_at": datetime.now(UTC),
                }
            )
    rows.sort(key=lambda r: (r["slot"], r.get("transaction_index") or 0), reverse=True)
    return rows[: max(1, int(limit))]


def solana_token_balances_for_accounts(
    src: SQDSource,
    accounts: list[str],
    from_slot: int,
    to_slot: int,
    *,
    limit: int = 500,
    max_batches: int = 48,
) -> list[dict[str, Any]]:
    """Token balance diffs for token accounts (``account`` filter is supported).

    Rows use the ``solana_transfers`` shape; ``tx_hash`` is empty
    (not part of the portal record).
    """
    if not accounts:
        return []
    fields = {
        "block": {"number": True, "timestamp": True},
        "tokenBalance": {
            "account": True,
            "preMint": True,
            "postMint": True,
            "preAmount": True,
            "postAmount": True,
            "preOwner": True,
            "postOwner": True,
            "transactionIndex": True,
        },
    }
    blocks = src.stream(
        "solana-mainnet",
        from_slot,
        to_slot,
        fields=fields,
        selectors={"tokenBalances": [{"account": list(accounts)}]},
        max_batches=max_batches,
    )
    seen: set[tuple[Any, ...]] = set()
    rows: list[dict[str, Any]] = []
    for block in blocks:
        header = block.get("header", {})
        slot = int(header.get("number", 0))
        block_ts = ts(header.get("timestamp"))
        for rec in block.get("tokenBalances", []) or []:
            pre = int(rec.get("preAmount") or 0)
            post = int(rec.get("postAmount") or 0)
            key = (slot, rec.get("transactionIndex"), rec.get("account"), pre, post)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "chain": "solana",
                    "slot": slot,
                    "block_timestamp": block_ts,
                    "tx_hash": "",
                    "token_account": rec.get("account") or "",
                    "mint": rec.get("postMint") or rec.get("preMint") or "",
                    "owner": rec.get("postOwner") or rec.get("preOwner") or "",
                    "pre_amount": str(pre),
                    "post_amount": str(post),
                    "delta": str(post - pre),
                    "decimals": 0,
                    "token_symbol": "",
                    "token_name": "",
                    "ingested_at": datetime.now(UTC),
                }
            )
    rows.sort(key=lambda r: (r["slot"], str(r.get("token_account"))), reverse=True)
    return rows[: max(1, int(limit))]
