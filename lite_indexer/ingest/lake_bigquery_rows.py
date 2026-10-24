"""Row mappers and the batch sink for the ethereum BigQuery fill.

Split out of :mod:`lake_bigquery_eth` on 2026-10-25 (200-line cap).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

_CHAIN = "ethereum"


def _ms_iso(value: Any) -> str:
    """Epoch-ms (BigQuery UNIX_MILLIS) → ISO-8601 UTC string."""
    try:
        return datetime.fromtimestamp(int(value) / 1000, UTC).isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def _addr(value: Any) -> str:
    return str(value or "").lower()


def map_transfer(raw: dict[str, Any]) -> dict[str, Any] | None:
    """``token_transfers`` row → ``evm_transfers`` event dict (None = NULL value)."""
    if raw.get("value") is None:
        return None
    return {
        "blockNumber": int(raw.get("block_number") or 0),
        "txHash": raw.get("tx_hash") or "",
        "from": _addr(raw.get("from_address")),
        "to": _addr(raw.get("to_address")),
        "tokenAddress": _addr(raw.get("token_address")),
        "value": str(raw["value"]),
        "blockTimestamp": _ms_iso(raw.get("block_timestamp")),
        "logIndex": int(raw.get("log_index") or 0),
    }


def _parse_trace_address(path_raw: object) -> list[int]:
    """``"0,1,2"`` -> ``[0, 1, 2]``; anything unparsable yields ``[]``."""
    path = str(path_raw or "").strip()
    if not path:
        return []
    return [int(p) for p in path.split(",") if p.strip().isdigit()]


def _parse_trace_value(value: object) -> int:
    """Trace value in wei. A NULL value is a genuine zero-value call."""
    return int(value) if value is not None else 0


def map_trace(raw: dict[str, Any]) -> dict[str, Any]:
    """``traces`` row → ``internal_transfers`` row dict."""
    trace_address = _parse_trace_address(raw.get("trace_address"))
    data = raw.get("input") or ""
    value = _parse_trace_value(raw.get("value"))
    return {
        "chain": _CHAIN,
        "block_number": int(raw.get("block_number") or 0),
        "block_timestamp": _ms_iso(raw.get("block_timestamp")),
        "tx_hash": raw.get("tx_hash") or "",
        "trace_address": trace_address,
        "from_address": _addr(raw.get("from_address")),
        "to_address": _addr(raw.get("to_address")),
        "value_wei": value,
        "call_type": raw.get("call_type") or raw.get("trace_type") or "",
        "gas_used": int(raw.get("gas_used") or 0),
        "error": str(raw.get("error") or "")[:200],
        "input_method_id": data[:10] if isinstance(data, str) and data.startswith("0x") else "",
    }


class _Sink:
    """Batch writer bound to one (chain, table) plus row/null counters."""

    def __init__(self, ch: Any, table: str, batch_rows: int) -> None:
        self._ch = ch
        self._table = table
        self._size = max(1, batch_rows)
        self._buf: list[dict[str, Any]] = []
        self.rows = 0
        self.nulls = 0

    def add(self, raw: dict[str, Any]) -> None:
        if self._table == "token_transfers":
            row = map_transfer(raw)
            if row is None:
                self.nulls += 1
                return
        else:
            if raw.get("value") is None:
                self.nulls += 1
            row = map_trace(raw)
        self._buf.append(row)
        self.rows += 1
        if len(self._buf) >= self._size:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        batch, self._buf = self._buf, []
        if self._table == "token_transfers":
            self._ch.write_transfers(_CHAIN, batch)
            return
        from lite_indexer.storage.trace_store import write_internal_transfers

        write_internal_transfers(self._ch.client(), _CHAIN, batch)
