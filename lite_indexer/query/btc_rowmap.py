"""Map AWS public-blockchain BTC rows onto our warehouse shape — honestly.

BTC is UTXO, not an account model. A transaction has ``inputs`` and ``outputs``
and **no** ``from_address``/``to_address``. There is no honest way to invent
that pair: one transaction can spend 30 inputs into 200 outputs, and Bitcoin
records no per-edge (input -> output) link at all. The mapping below therefore
keeps the UTXO shape and names the lossy parts instead of hiding them:

1. **No from/to.** ``input_addresses`` / ``output_addresses`` are lists. The
   *edges* exist only at two coarser levels: ``input_spent_tx`` (which parent
   transaction each input came from) and block membership. Any caller that
   needs (from, to) must join a transaction inputs to its outputs, and that
   join is ambiguous by construction — see :data:`UTXO_EDGE_LOSS`.
2. **Values in sats, not BTC floats.** The lake stores ``DOUBLE`` BTC. Total
   supply is 2.1e15 sats, and ``2**53`` sats is 9.0e16 sats, so every legal
   amount is exactly representable after ``round(value * 1e8)``. BTC has at
   most 8 decimals, so the round is not a rounding decision, it is a unit
   change. Keeping the float would import artefacts like
   ``0.30595074000000005`` into the archive.
3. **NULL address stays NULL.** A coinbase row has no input list (nothing is
   spent); an OP_RETURN / non-standard output carries no address. Coercing
   either to ``""`` would fabricate an address that does not exist, and every
   downstream unknown-address aggregate would be wrong.
4. **Coinbase has no inputs.** ``input_count`` is 0 and the input lists are
   NULL, so value flow from nowhere is flagged by ``is_coinbase`` rather than
   by a synthetic zero-value input.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

#: Sats per BTC — the unit change described in the module docstring.
SATS_PER_BTC = 100_000_000

#: What the UTXO shape cannot express, stated once for callers and docs.
UTXO_EDGE_LOSS = (
    "no (from_address, to_address) pair: BTC records inputs and outputs as "
    "separate lists with no per-edge link; a fan-out transaction has no "
    "attributable sender->recipient amount"
)

_TX_TABLES = frozenset({"btc_transactions", "transactions"})
_BLOCK_TABLES = frozenset({"btc_blocks", "blocks"})


def sats(value: Any) -> int | None:
    """BTC double -> integer sats, preserving NULL.

    Args:
        value: Lake ``DOUBLE`` BTC amount, or ``None``.

    Returns:
        ``None`` for NULL (unknown), otherwise integer sats.
    """
    if value is None:
        return None
    return int(round(float(value) * SATS_PER_BTC))


def _str_list(value: Any) -> list[str | None] | None:
    """Struct-list -> plain list, keeping NULLs positionally aligned."""
    if value is None:
        return None
    return [None if item is None else str(item) for item in value]


def _tx_row(raw: dict[str, Any], now: datetime) -> dict[str, Any]:
    """One BTC transaction, UTXO shape preserved."""
    in_vals, out_vals = raw.get("input_values"), raw.get("output_values")
    spent_ix = raw.get("input_spent_index")
    return {
        "chain": "btc",
        "table": "btc_transactions",
        "block_number": int(raw.get("block_number") or 0),
        "block_hash": raw.get("block_hash") or "",
        "block_timestamp": raw.get("block_timestamp"),
        "tx_hash": raw.get("tx_hash") or "",
        "tx_index": int(raw.get("tx_index") or 0),
        "is_coinbase": bool(raw.get("is_coinbase")),
        "input_count": int(raw.get("input_count") or 0),
        "output_count": int(raw.get("output_count") or 0),
        "fee_sats": sats(raw.get("fee")),
        "input_addresses": _str_list(raw.get("input_addresses")),
        "input_value_sats": None if in_vals is None else [sats(v) for v in in_vals],
        "input_spent_tx": _str_list(raw.get("input_spent_tx")),
        "input_spent_index": None if spent_ix is None else [int(i) if i is not None else None for i in spent_ix],
        "output_addresses": _str_list(raw.get("output_addresses")),
        "output_value_sats": None if out_vals is None else [sats(v) for v in out_vals],
        "output_types": _str_list(raw.get("output_types")),
        "utxo_edge_loss": UTXO_EDGE_LOSS,
        "ingested_at": now,
    }


def _block_row(raw: dict[str, Any], now: datetime) -> dict[str, Any]:
    return {
        "chain": "btc",
        "table": "btc_blocks",
        "block_number": int(raw.get("block_number") or 0),
        "block_hash": raw.get("block_hash") or "",
        "block_timestamp": raw.get("block_timestamp"),
        "previous_block_hash": raw.get("previousblockhash") or "",
        "transaction_count": int(raw.get("transaction_count") or 0),
        "size_bytes": int(raw.get("size") or 0),
        "weight": int(raw.get("weight") or 0),
        "difficulty": raw.get("difficulty"),
        "bits": str(raw.get("bits") or ""),
        "chainwork": str(raw.get("chainwork") or ""),
        "merkle_root": raw.get("merkle_root") or "",
        "version": int(raw.get("version") or 0),
        "nonce": int(raw.get("nonce") or 0),
        "ingested_at": now,
    }


def map_row(table: str, raw: dict[str, Any]) -> dict[str, Any]:
    """Map one lake row to the warehouse row shape for ``table``.

    Args:
        table: Logical table name (``btc_transactions`` / ``btc_blocks``,
            or the raw lake names ``transactions`` / ``blocks``).
        raw: One row as a dict of lake columns.

    Returns:
        The warehouse row.

    Raises:
        ValueError: When ``table`` is not a mapped BTC table. Guessing a
            shape is how a wrong column silently reaches the archive.
    """
    now = datetime.now(UTC)
    if table in _BLOCK_TABLES:
        return _block_row(raw, now)
    if table in _TX_TABLES:
        return _tx_row(raw, now)
    raise ValueError(f"unmapped btc table: {table!r}")


__all__ = ["SATS_PER_BTC", "UTXO_EDGE_LOSS", "map_row", "sats"]
