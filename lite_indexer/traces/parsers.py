"""
Trace parsers — normalize trace_* RPC responses into internal-transfer rows.

Two wire formats are handled:

1. ``debug_traceBlockByNumber`` with ``{"tracer": "callTracer"}`` (geth):
   a list of per-transaction entries, each with a nested ``calls`` tree::

       [
         {
           "txHash": "0x...",
           "result": {
             "type": "CALL", "from": "0x...", "to": "0x...",
             "value": "0x...", "gasUsed": "0x...", "input": "0x...",
             "calls": [ { ... nested ... } ]
           }
         }
       ]

2. ``trace_block`` (parity / erigon): a flat list of trace objects with an
   ``action`` + ``result`` pair and an explicit ``traceAddress``::

       [
         {
           "type": "call", "transactionHash": "0x...",
           "action": {"callType": "call", "from": "0x...", "to": "0x...",
                      "value": "0x...", "input": "0x..."},
           "result": {"gasUsed": "0x...", "output": "0x..."},
           "traceAddress": [0, 1]
         }
       ]

Extraction rule (avoids double counting): only *value-moving* frames are
emitted — ``CALL`` with ``value > 0``, ``CREATE``/``CREATE2`` with
``value > 0`` (funding a new contract), and ``SELFDESTRUCT`` (balance paid
out to the beneficiary). ``DELEGATECALL``/``STATICCALL`` frames are
skipped even when they carry a ``value`` field: a delegatecall executes in
the caller's context and its ``value`` is inherited context, not a new
transfer — recording it would duplicate the parent CALL row. ``reward``
traces (block/uncle rewards) are consensus-level, not internal transfers,
and are also skipped.

Rows follow the ``wallet_memory.internal_transfers`` schema:

    chain, block_number, block_timestamp, tx_hash, trace_address,
    from_address, to_address, value_wei, call_type, gas_used, error,
    input_method_id
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

# Transaction method selector prefixes that identify common value-moving
# flows. The full selector lives in ``input_method_id``; these are just
# constants used by tests/analysis to spot WETH unwraps etc.
WETH_WITHDRAW_SELECTOR = "0x2e1a7d4d"  # WETH.withdraw(uint256)
ERC20_TRANSFER_SELECTOR = "0xa9059cbb"  # transfer(address,uint256)


def input_method_id(input_hex: str | None) -> str:
    """Return the first 4 bytes of call data as ``0x``-prefixed hex.

    Args:
        input_hex: Hex-encoded call data (may be ``None`` or empty).

    Returns:
        The method selector (e.g. ``0x2e1a7d4d`` for WETH.withdraw),
        or ``''`` when there is no call data (plain ETH transfer,
        contract creation init code, etc.).
    """
    if not input_hex or not input_hex.startswith("0x") or len(input_hex) < 10:
        return ""
    return "0x" + input_hex[2:10].lower()


def _normalize_timestamp(block_timestamp: datetime | str | None) -> datetime | None:
    """Coerce a block timestamp into a timezone-aware datetime.

    Args:
        block_timestamp: datetime, ISO-8601 string, or None.

    Returns:
        Normalized datetime (UTC) or None when the input was None/empty.
    """
    if block_timestamp is None:
        return None
    if isinstance(block_timestamp, datetime):
        if block_timestamp.tzinfo is None:
            return block_timestamp.replace(tzinfo=UTC)
        return block_timestamp
    try:
        return datetime.fromisoformat(str(block_timestamp).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _make_row(
    *,
    chain: str,
    block_number: int,
    block_timestamp: datetime | str | None,
    tx_hash: str,
    trace_address: list[int],
    from_address: str,
    to_address: str,
    value_wei: int,
    call_type: str,
    gas_used: int,
    error: str,
    input_hex: str | None,
) -> dict[str, Any]:
    """Build a standardized internal-transfer row.

    Args:
        chain: Chain name (e.g. 'ethereum').
        block_number: Block number.
        block_timestamp: Block timestamp (datetime or ISO string).
        tx_hash: Transaction hash ('' for reward/consensus traces).
        trace_address: Nested call path ([] for top-level frames).
        from_address: Sender (lowercased).
        to_address: Recipient (lowercased; created address for CREATE).
        value_wei: Value transferred in wei.
        call_type: 'call', 'create', or 'selfdestruct'.
        gas_used: Gas consumed by the frame.
        error: Revert/error message ('' on success).
        input_hex: Raw call data (used for input_method_id).

    Returns:
        Standardized dict matching the internal_transfers schema.
    """
    return {
        "chain": chain,
        "block_number": block_number,
        "block_timestamp": _normalize_timestamp(block_timestamp),
        "tx_hash": tx_hash,
        "trace_address": list(trace_address),
        "from_address": (from_address or "").lower(),
        "to_address": (to_address or "").lower(),
        "value_wei": value_wei,
        "call_type": call_type,
        "gas_used": gas_used,
        "error": error or "",
        "input_method_id": input_method_id(input_hex),
    }


def _walk_call_tracer(
    frame: dict[str, Any],
    *,
    chain: str,
    block_number: int,
    block_timestamp: datetime | str | None,
    tx_hash: str,
    path: list[int],
    rows: list[dict[str, Any]],
) -> None:
    """Recursively walk one callTracer call tree, emitting value transfers.

    The root frame of a transaction has ``trace_address = []``; each
    nested call appends its index in the parent's ``calls`` list (matching
    parity's traceAddress semantics).

    Args:
        frame: callTracer frame dict.
        chain: Chain name.
        block_number: Block number.
        block_timestamp: Block timestamp.
        tx_hash: Transaction hash for this call tree.
        path: Current trace address path.
        rows: Output list (mutated in place).
    """
    ftype = str(frame.get("type") or "CALL").upper()
    value_wei = int(str(frame.get("value") or "0x0"), 16)
    gas_used = int(str(frame.get("gasUsed") or "0x0"), 16)
    error = str(frame.get("error") or "")
    input_hex = frame.get("input")

    if ftype in ("CALL", "CREATE", "CREATE2") and value_wei > 0:
        call_type = "create" if ftype in ("CREATE", "CREATE2") else "call"
        rows.append(
            _make_row(
                chain=chain,
                block_number=block_number,
                block_timestamp=block_timestamp,
                tx_hash=tx_hash,
                trace_address=path,
                from_address=str(frame.get("from") or ""),
                to_address=str(frame.get("to") or ""),
                value_wei=value_wei,
                call_type=call_type,
                gas_used=gas_used,
                error=error,
                # CREATE frames carry init code, not a method call.
                input_hex=None if call_type == "create" else input_hex,
            )
        )
    elif ftype == "SELFDESTRUCT":
        # geth callTracer emits SELFDESTRUCT frames with from = the
        # contract, to = the beneficiary, value = the contract balance.
        rows.append(
            _make_row(
                chain=chain,
                block_number=block_number,
                block_timestamp=block_timestamp,
                tx_hash=tx_hash,
                trace_address=path,
                from_address=str(frame.get("from") or ""),
                to_address=str(frame.get("to") or ""),
                value_wei=value_wei,
                call_type="selfdestruct",
                gas_used=gas_used,
                error=error,
                input_hex=input_hex,
            )
        )
    # DELEGATECALL / STATICCALL / zero-value CALL: no new value movement —
    # still recurse so nested value-moving frames are found.

    for index, child in enumerate(frame.get("calls") or []):
        if isinstance(child, dict):
            _walk_call_tracer(
                child,
                chain=chain,
                block_number=block_number,
                block_timestamp=block_timestamp,
                tx_hash=tx_hash,
                path=path + [index],
                rows=rows,
            )


def parse_call_tracer(
    trace_results: list[Any],
    chain: str,
    block_number: int,
    block_timestamp: datetime | str | None = None,
) -> list[dict[str, Any]]:
    """Parse a ``debug_traceBlockByNumber`` callTracer response.

    Args:
        trace_results: The ``result`` array from the RPC response.
        chain: Chain name.
        block_number: Block number being traced.
        block_timestamp: Block timestamp (datetime or ISO string).

    Returns:
        List of standardized internal-transfer rows.
    """
    rows: list[dict[str, Any]] = []
    for entry in trace_results:
        if not isinstance(entry, dict):
            continue
        tx_hash = str(entry.get("txHash") or "")
        root = entry.get("result")
        if not isinstance(root, dict):
            continue
        _walk_call_tracer(
            root,
            chain=chain,
            block_number=block_number,
            block_timestamp=block_timestamp,
            tx_hash=tx_hash,
            path=[],
            rows=rows,
        )
    return rows


def _parse_parity_trace(
    trace: dict[str, Any],
    *,
    chain: str,
    block_number: int,
    block_timestamp: datetime | str | None,
) -> dict[str, Any] | None:
    """Parse a single parity trace_block entry into a row (or None).

    Args:
        trace: Parity trace dict (type + action + result).
        chain: Chain name.
        block_number: Block number.
        block_timestamp: Block timestamp.

    Returns:
        Standardized row for value-moving frames, else None.
    """
    trace_type = trace.get("type")
    action = trace.get("action") or {}
    tx_hash = str(trace.get("transactionHash") or "")
    path = [int(i) for i in (trace.get("traceAddress") or [])]
    error = str(trace.get("error") or "")
    result = trace.get("result") or {}

    if trace_type == "call":
        if action.get("callType") != "call":
            return None  # delegatecall/staticcall — no value movement
        value_wei = int(str(action.get("value") or "0x0"), 16)
        if value_wei <= 0:
            return None
        return _make_row(
            chain=chain,
            block_number=block_number,
            block_timestamp=block_timestamp,
            tx_hash=tx_hash,
            trace_address=path,
            from_address=str(action.get("from") or ""),
            to_address=str(action.get("to") or ""),
            value_wei=value_wei,
            call_type="call",
            gas_used=int(str(result.get("gasUsed") or "0x0"), 16),
            error=error,
            input_hex=action.get("input"),
        )

    if trace_type == "create":
        value_wei = int(str(action.get("value") or "0x0"), 16)
        if value_wei <= 0:
            return None
        return _make_row(
            chain=chain,
            block_number=block_number,
            block_timestamp=block_timestamp,
            tx_hash=tx_hash,
            trace_address=path,
            from_address=str(action.get("from") or ""),
            # The created contract address is in result.address for parity.
            to_address=str(result.get("address") or ""),
            value_wei=value_wei,
            call_type="create",
            gas_used=int(str(result.get("gasUsed") or "0x0"), 16),
            error=error,
            input_hex=None,  # init code, not a method call
        )

    if trace_type == "suicide":
        # Parity suicide: action = {address, balance, refundAddress}.
        return _make_row(
            chain=chain,
            block_number=block_number,
            block_timestamp=block_timestamp,
            tx_hash=tx_hash,
            trace_address=path,
            from_address=str(action.get("address") or ""),
            to_address=str(action.get("refundAddress") or ""),
            value_wei=int(str(action.get("balance") or "0x0"), 16),
            call_type="selfdestruct",
            gas_used=0,
            error=error,
            input_hex=None,
        )

    # type == "reward" and anything else: not an internal transfer.
    return None


def parse_parity_trace_block(
    trace_results: list[Any],
    chain: str,
    block_number: int,
    block_timestamp: datetime | str | None = None,
) -> list[dict[str, Any]]:
    """Parse a ``trace_block`` (parity/erigon) response.

    Args:
        trace_results: The ``result`` array from the RPC response.
        chain: Chain name.
        block_number: Block number being traced.
        block_timestamp: Block timestamp (datetime or ISO string).

    Returns:
        List of standardized internal-transfer rows.
    """
    rows: list[dict[str, Any]] = []
    for trace in trace_results:
        if not isinstance(trace, dict):
            continue
        row = _parse_parity_trace(
            trace,
            chain=chain,
            block_number=block_number,
            block_timestamp=block_timestamp,
        )
        if row is not None:
            rows.append(row)
    return rows


def extract_internal_transfers(
    trace_results: list[Any],
    chain: str,
    block_number: int,
    block_timestamp: datetime | str | None = None,
) -> list[dict[str, Any]]:
    """Extract value-transferring internal calls from any supported format.

    Auto-detects the response format: parity ``trace_block`` entries have
    an ``action`` key; geth callTracer entries have a nested ``result``
    with ``calls``/``type`` keys.

    Args:
        trace_results: The ``result`` array from trace_block or
            debug_traceBlockByNumber (callTracer).
        chain: Chain name.
        block_number: Block number being traced.
        block_timestamp: Block timestamp (datetime or ISO string).

    Returns:
        List of standardized internal-transfer rows.
    """
    if not trace_results:
        return []
    first = trace_results[0]
    if isinstance(first, dict) and "action" in first:
        return parse_parity_trace_block(trace_results, chain, block_number, block_timestamp)
    return parse_call_tracer(trace_results, chain, block_number, block_timestamp)
