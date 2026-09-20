"""Tron readers + address helpers for the SQD Portal client.

TRC-20 transfers (ERC-20 ``Transfer`` topic) and native internal
transactions. Functions take the portal client (``src``) so the facade keeps
the public method names; the address helpers are re-exported on ``SQDSource``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lite_indexer.query.sqd_common import ERC20_TRANSFER_TOPIC, ts
from lite_indexer.query.sqd_tron_addr import tron_base58, tron_hex40, tron_topic

if TYPE_CHECKING:
    from lite_indexer.query.sqd_source import SQDSource


def tron_trc20_transfers(
    src: SQDSource,
    address: str,
    from_block: int,
    to_block: int,
    *,
    limit: int = 200,
    max_batches: int = 48,
) -> list[dict[str, Any]]:
    """TRC-20 transfers where ``address`` is sender or recipient.

    Logs are filtered by the (padded) wallet in ``topic1``/``topic2``
    with the ERC-20 ``Transfer`` topic — verified supported 2026-09-16.
    Rows use the ``evm_transfers`` shape (Tron hex addresses, base58
    token address to match the warehouse).
    """
    if not src.available():
        return []
    topic = tron_topic(address)
    topic0 = ERC20_TRANSFER_TOPIC[2:]  # Tron topics carry no 0x prefix
    fields = {
        "block": {"number": True, "timestamp": True},
        "log": {"address": True, "topics": True, "data": True, "transactionIndex": True},
    }
    seen: set[tuple[Any, ...]] = set()
    rows: list[dict[str, Any]] = []
    for side_key in ("topic1", "topic2"):
        blocks = src.stream(
            "tron-mainnet",
            from_block,
            to_block,
            fields=fields,
            selectors={"logs": [{"topic0": [topic0], side_key: [topic]}]},
            max_batches=max_batches,
        )
        for block in blocks:
            header = block.get("header", {})
            bn = int(header.get("number", 0))
            block_ts = ts(header.get("timestamp"))
            for rec in block.get("logs", []) or []:
                topics = rec.get("topics") or []
                if len(topics) < 3:
                    continue
                frm = ("0x" + str(topics[1])[-40:]).lower()
                to = ("0x" + str(topics[2])[-40:]).lower()
                data = rec.get("data") or ""
                try:
                    value = str(int(data, 16))
                except (TypeError, ValueError):
                    value = "0"
                key = (bn, rec.get("transactionIndex"), frm, to, data)
                if key in seen:
                    continue
                seen.add(key)
                try:
                    token = tron_base58(str(rec.get("address") or ""))
                except ValueError:
                    token = str(rec.get("address") or "")
                rows.append(
                    {
                        "chain": "tron",
                        "block_number": bn,
                        "tx_hash": "",
                        "from_address": frm,
                        "to_address": to,
                        "token_address": token,
                        "value": value,
                        "block_timestamp": block_ts,
                        "log_index": int(rec.get("transactionIndex") or 0),
                        "ingested_at": datetime.now(UTC),
                    }
                )
    rows.sort(key=lambda r: (r["block_number"], r["log_index"]), reverse=True)
    return rows[: max(1, int(limit))]


def tron_internal_transfers(
    src: SQDSource,
    address: str,
    from_block: int,
    to_block: int,
    *,
    limit: int = 200,
    max_batches: int = 48,
) -> list[dict[str, Any]]:
    """Native (TRX) internal transactions touching ``address``.

    Uses the ``transferTo`` / ``caller`` filters (41-prefixed hex) and
    maps to the ``internal_transfers`` row shape.
    """
    if not src.available():
        return []
    addr41 = "41" + tron_hex40(address)
    fields = {
        "block": {"number": True, "timestamp": True},
        "internalTransaction": {
            "callerAddress": True,
            "transferToAddress": True,
            "callValueInfo": True,
            "internalTransactionIndex": True,
            "transactionIndex": True,
            "rejected": True,
        },
    }
    seen: set[tuple[Any, ...]] = set()
    rows: list[dict[str, Any]] = []
    for key_name in ("transferTo", "caller"):
        blocks = src.stream(
            "tron-mainnet",
            from_block,
            to_block,
            fields=fields,
            selectors={"internalTransactions": [{key_name: [addr41]}]},
            max_batches=max_batches,
        )
        for block in blocks:
            header = block.get("header", {})
            bn = int(header.get("number", 0))
            block_ts = ts(header.get("timestamp"))
            for rec in block.get("internalTransactions", []) or []:
                caller = str(rec.get("callerAddress") or "")
                to = str(rec.get("transferToAddress") or "")
                value = 0
                for ci in rec.get("callValueInfo") or []:
                    try:
                        value += int(ci.get("callValue") or 0)
                    except (TypeError, ValueError):
                        continue
                trace = f"{rec.get('transactionIndex', 0)}.{rec.get('internalTransactionIndex', 0)}"
                rk = (bn, trace, caller, to)
                if rk in seen:
                    continue
                seen.add(rk)
                rows.append(
                    {
                        "chain": "tron",
                        "block_number": bn,
                        "block_timestamp": block_ts,
                        "tx_hash": "",
                        "trace_address": trace,
                        "from_address": ("0x" + caller[2:]).lower() if caller.startswith("41") else caller.lower(),
                        "to_address": ("0x" + to[2:]).lower() if to.startswith("41") else to.lower(),
                        "value_wei": str(value),
                        "call_type": "call",
                        "gas_used": 0,
                        "error": "rejected" if rec.get("rejected") else "",
                        "input_method_id": "",
                        "ingested_at": datetime.now(UTC),
                    }
                )
    rows.sort(key=lambda r: (r["block_number"], r["trace_address"]), reverse=True)
    return rows[: max(1, int(limit))]
