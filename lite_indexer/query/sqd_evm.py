"""EVM full-history trace reader for the SQD Portal client.

Maps portal ``trace`` records (``action``/``result`` nesting) to the
``internal_transfers`` row shape across the ~13 EVM chains the traces
dataset covers.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lite_indexer.query.sqd_common import ts

if TYPE_CHECKING:
    from lite_indexer.query.sqd_source import SQDSource

TRACE_FIELDS: dict[str, Any] = {
    "block": {"number": True, "timestamp": True},
    "trace": {
        "transactionIndex": True,
        "traceAddress": True,
        "subtraces": True,
        "type": True,
        "error": True,
        "revertReason": True,
        "callFrom": True,
        "callTo": True,
        "callValue": True,
        "callGas": True,
        "callInput": True,
        "callResultGasUsed": True,
    },
}


def map_trace(chain: str, block: dict[str, Any], rec: dict[str, Any]) -> dict[str, Any]:
    """Map one portal trace record (``action``/``result`` nesting) to a row."""
    header = block.get("header", {})
    action = rec.get("action") or {}
    result = rec.get("result") or {}
    trace_address = rec.get("traceAddress") or []
    value_raw = action.get("value") or "0x0"
    try:
        value = str(int(str(value_raw), 16))
    except (TypeError, ValueError):
        value = str(value_raw)
    gas_raw = result.get("gasUsed") or "0x0"
    try:
        gas_used = int(str(gas_raw), 16)
    except (TypeError, ValueError):
        gas_used = 0
    input_raw = str(action.get("input") or "")
    return {
        "chain": chain,
        "block_number": int(header.get("number") or 0),
        "block_timestamp": ts(header.get("timestamp")),
        "tx_hash": "",
        "trace_address": ".".join(str(x) for x in trace_address),
        "from_address": str(action.get("from") or "").lower(),
        "to_address": str(action.get("to") or "").lower(),
        "value_wei": value,
        "call_type": str(rec.get("callType") or rec.get("type") or ""),
        "gas_used": gas_used,
        "error": "" if not rec.get("error") else str(rec.get("error"))[:200],
        "input_method_id": input_raw[:10] if input_raw.startswith("0x") else "",
        "ingested_at": datetime.now(UTC),
    }


def evm_traces(
    src: SQDSource,
    chain: str,
    from_block: int,
    to_block: int,
    *,
    address: str | None = None,
    limit: int = 200,
    max_batches: int = 48,
) -> list[dict[str, Any]]:
    """Full-history EVM execution traces → ``internal_transfers`` row shape.

    The SQD portal traces dataset covers ~13 EVM chains with complete
    history (blocks from 2021+ verified) and supports ``callFrom`` /
    ``callTo`` server-side address filters. ``tx_hash`` is empty (portal
    traces carry only the transaction index).
    """
    if not src.available() or chain not in src.EVM_DATASETS:
        return []
    dataset = src.EVM_DATASETS[chain]
    addr = str(address or "").strip().lower()
    selectors_list: list[dict[str, Any]] = [{"callFrom": [addr]}, {"callTo": [addr]}] if addr else [{}]
    seen: set[tuple[Any, ...]] = set()
    rows: list[dict[str, Any]] = []
    for selector in selectors_list:
        blocks = src.stream(
            dataset,
            from_block,
            to_block,
            fields=TRACE_FIELDS,
            selectors={"traces": [selector]},
            max_batches=max_batches,
        )
        for block in blocks:
            header_number = block.get("header", {}).get("number")
            for rec in block.get("traces", []) or []:
                trace_address = rec.get("traceAddress") or []
                key = (
                    header_number,
                    rec.get("transactionIndex"),
                    ".".join(str(x) for x in trace_address),
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(map_trace(chain, block, rec))
    rows.sort(key=lambda r: (r["block_number"], r["trace_address"]), reverse=True)
    return rows[: max(1, int(limit))]
