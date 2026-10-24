"""Solana edges via a keyed archive tier (enhanced transactions, SOL + SPL).

Solana is the one chain where our own tables cannot answer the funding
question: ``solana_spl_balance_changes`` has no counterparty column (only
``owner`` + ``delta``), and native SOL movements are not stored at all. So
the trace reads the archive tier's parsed transactions, which carry
``nativeTransfers`` (lamports) and ``tokenTransfers`` (human amounts) with
both sides — the same shape the EVM tier produces.

The tier is keyed and its base URL is operator-supplied, both from the
private runtime env (never git): ``SOLANA_TRACE_API_KEY`` and
``SOLANA_TRACE_API_URL`` (a template containing ``{address}``). Without
either the tier reports itself unavailable rather than guessing.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

from lite_indexer.env import load_env_file
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_URL_ENV = "SOLANA_TRACE_API_URL"
_KEY_ENV = "SOLANA_TRACE_API_KEY"
_LAMPORTS = 1_000_000_000


def _api_key() -> str:
    load_env_file()
    return os.getenv(_KEY_ENV, "").strip()


def _api_url() -> str:
    load_env_file()
    return os.getenv(_URL_ENV, "").strip()


def _get(url: str, timeout: int) -> Any:
    request = urllib.request.Request(url, headers={"accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 — fixed https host
        return json.loads(response.read().decode("utf-8"))


def _edge(
    src: str,
    dst: str,
    *,
    asset: str,
    amount: float,
    symbol: str,
    signature: str,
    ts: str,
    kind: str,
) -> dict[str, Any]:
    return {
        "from": src,
        "to": dst,
        "asset": asset,
        "raw": 0,
        "amount": amount,
        "symbol": symbol,
        "tx_hash": signature,
        "ts": ts,
        "kind": kind,
        "tier": "solana-archive",
        "tx_count": 1,
    }


def _transfers(payload: list[dict[str, Any]], address: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for tx in payload:
        signature = str(tx.get("signature") or "")
        stamp = tx.get("timestamp")
        ts = datetime.fromtimestamp(int(stamp), tz=UTC).isoformat() if stamp else ""
        for move in tx.get("nativeTransfers") or []:
            src = str(move.get("fromUserAccount") or "")
            dst = str(move.get("toUserAccount") or "")
            if address not in (src, dst):
                continue
            amount = float(move.get("amount") or 0) / _LAMPORTS
            if amount <= 0:
                continue
            rows.append(
                _edge(
                    src,
                    dst,
                    asset="native",
                    amount=amount,
                    symbol="SOL",
                    signature=signature,
                    ts=ts,
                    kind="sol",
                )
            )
        for move in tx.get("tokenTransfers") or []:
            src = str(move.get("fromUserAccount") or "")
            dst = str(move.get("toUserAccount") or "")
            if address not in (src, dst):
                continue
            amount = float(move.get("tokenAmount") or 0)
            if amount <= 0:
                continue
            mint = str(move.get("mint") or "")
            rows.append(
                _edge(
                    src,
                    dst,
                    asset=mint,
                    amount=amount,
                    symbol="",
                    signature=signature,
                    ts=ts,
                    kind="spl",
                )
            )
    return rows


def fetch_solana(
    address: str,
    *,
    limit: int = 100,
    timeout: int = 25,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Return native + SPL edges for one Solana address via the archive tier.

    Args:
        address: Base58 account address (case-sensitive).
        limit: Transactions requested (tier max 100 per call).
        timeout: HTTP timeout in seconds.

    Returns:
        ``(rows, notes)``. Rows only include transfers where ``address`` is
        one side, and only the newest ``limit`` transactions — the tier's
        address endpoint is recency-ordered, so this is a *recent* view, not
        full history.
    """
    key = _api_key()
    template = _api_url()
    if not key or not template:
        return [], [f"solana-archive: unconfigured ({_KEY_ENV} / {_URL_ENV})"]
    query = urllib.parse.urlencode({"api-key": key, "limit": max(1, min(int(limit), 100))})
    url = f"{template.format(address=urllib.parse.quote(address))}?{query}"
    try:
        payload = _get(url, timeout)
    except Exception as exc:  # noqa: BLE001 — a dead RPC tier is a note, not a crash
        logger.warning("solana_trace_failed", error=str(exc)[:120])
        return [], [f"solana-archive: request failed ({str(exc)[:80]})"]
    if not isinstance(payload, list):
        return [], ["solana-archive: unexpected payload shape"]
    rows = _transfers(payload, address)
    notes = [f"solana-archive: {len(payload)} txs scanned, {len(rows)} movements"]
    return rows, notes
