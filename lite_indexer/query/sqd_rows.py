"""Portal records → archive row shapes for the SQD backfill.

The archive's row identity is not the portal's. Two gaps have to be closed here
rather than in a consumer:

1. **A trace has no transaction hash.** Portal ``trace`` records carry only
   ``transactionIndex``, while ``internal_transfers`` is deduped on
   ``(chain, block_number, tx_hash, trace_address)``. An empty ``tx_hash``
   collapses that key. The fix is to ask for the sibling ``transactions`` field
   on the SAME request — its array index equals ``transactionIndex``.
   Measured on base-mainnet block 30000000: 1006 transactions, 5922 traces,
   zero indices out of range.

2. **A trace is not a transfer.** ``migrations/004_internal_transfers.sql``
   stores only value-moving frames ("DELEGATECALL/STATICCALL frames are
   intentionally NOT stored — they do not move value"). Measured on polygon
   blocks 60000000-60000005: 2067 traces, 4.0% value-moving. Keeping the other
   96% would inflate every unit ~25x for rows no reader asks for.

WHAT IS DELIBERATELY SKIPPED
    ERC-721 ``Transfer`` logs (4 topics, ``tokenId`` in ``topics[3]``) and any
    log whose data is not a single 32-byte word are dropped, not coerced: an
    ``evm_transfers`` row with a tokenId in ``value`` is worse than no row.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Iterator
from typing import Any

from lite_indexer.query.sqd_common import ts

#: ERC-20 ``Transfer`` event topic — the selector that defines ``evm_transfers``.
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

#: Portal fields for an ``evm_transfers`` unit.
LOG_FIELDS: dict[str, Any] = {
    "block": {"number": True, "timestamp": True},
    "log": {
        "address": True,
        "topics": True,
        "data": True,
        "transactionHash": True,
        "logIndex": True,
    },
}

#: Portal selector for ``evm_transfers`` — the Transfer topic, nothing else.
LOG_SELECTOR: dict[str, Any] = {"logs": [{"topic0": [ERC20_TRANSFER_TOPIC]}]}

#: Trace frame types that can move value.
MOVING_TYPES = frozenset({"call", "create", "selfdestruct", "suicide"})


def hex_int(value: Any) -> int:
    """Parse a hex or decimal string to int; 0 when unparseable."""
    try:
        return int(str(value), 0)
    except (TypeError, ValueError):
        return 0


def tx_hashes(block: dict[str, Any]) -> list[str]:
    """Transaction hashes of one block, in ``transactionIndex`` order."""
    return [str((tx or {}).get("hash") or "") for tx in (block.get("transactions") or [])]


def trace_row(chain: str, block: dict[str, Any], hashes: list[str], rec: dict[str, Any]) -> dict[str, Any] | None:
    """One portal trace → one ``internal_transfers`` row, or None to skip."""
    call_type = str(rec.get("type") or "")
    if call_type not in MOVING_TYPES:
        return None
    action = rec.get("action") or {}
    value = hex_int(action.get("value") or "0x0")
    if call_type in ("call", "create") and value <= 0:
        return None
    header = block.get("header") or {}
    index = rec.get("transactionIndex")
    hashed = hashes[index] if isinstance(index, int) and 0 <= index < len(hashes) else ""
    input_raw = str(action.get("input") or "")
    result = rec.get("result") or {}
    return {
        "chain": chain,
        "block_number": int(header.get("number") or 0),
        "block_timestamp": ts(header.get("timestamp")),
        "tx_hash": hashed,
        "trace_address": ".".join(str(x) for x in (rec.get("traceAddress") or [])),
        "from_address": str(action.get("from") or "").lower(),
        "to_address": str(action.get("to") or "").lower(),
        "value_wei": str(value),
        "call_type": call_type,
        "gas_used": hex_int(result.get("gasUsed") or "0x0"),
        "error": "" if not rec.get("error") else str(rec.get("error"))[:200],
        "input_method_id": input_raw[:10] if input_raw.startswith("0x") else "",
        "ingested_at": dt.datetime.now(dt.UTC),
    }


def topic_address(topic: Any) -> str:
    """Unpad a 32-byte indexed address topic to a 20-byte address."""
    raw = str(topic or "").removeprefix("0x")
    return "0x" + raw[-40:].lower() if len(raw) >= 40 else ""


def log_row(chain: str, block: dict[str, Any], rec: dict[str, Any]) -> dict[str, Any] | None:
    """One ERC-20 ``Transfer`` log → one ``evm_transfers`` row, or None to skip."""
    topics = rec.get("topics") or []
    if len(topics) != 3:
        return None
    data = str(rec.get("data") or "").removeprefix("0x")
    if len(data) < 64:
        return None
    header = block.get("header") or {}
    return {
        "chain": chain,
        "block_number": int(header.get("number") or 0),
        "tx_hash": str(rec.get("transactionHash") or ""),
        "from_address": topic_address(topics[1]),
        "to_address": topic_address(topics[2]),
        "token_address": str(rec.get("address") or "").lower(),
        "value": str(int(data[:64], 16)),
        "block_timestamp": ts(header.get("timestamp")),
        "log_index": hex_int(rec.get("logIndex")),
        "ingested_at": dt.datetime.now(dt.UTC),
    }


def blocks_to_rows(chain: str, table: str, blocks: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Yield mapped rows for ``table`` from portal block objects."""
    for block in blocks:
        if table == "internal_transfers":
            hashes = tx_hashes(block)
            for rec in block.get("traces") or []:
                row = trace_row(chain, block, hashes, rec)
                if row is not None:
                    yield row
        else:
            for rec in block.get("logs") or []:
                row = log_row(chain, block, rec)
                if row is not None:
                    yield row
