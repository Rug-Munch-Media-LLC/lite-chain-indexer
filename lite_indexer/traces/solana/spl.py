"""SPL token transfer extraction helpers.

Split out of the former monolithic ``traces/solana.py`` (939 lines).
"""

from __future__ import annotations

from typing import Any

from .helpers import _known_decimals, _to_int


def _token_amount(parsed_info: dict[str, Any]) -> tuple[str, int]:
    """Extract (raw_amount, decimals) from a parsed token instruction.

    jsonParsed token instructions carry ``tokenAmount`` (with
    ``amount`` + ``decimals``) for checked transfers and most transfers;
    plain ``transfer`` may only carry ``amount``.

    Args:
        parsed_info: The instruction's ``parsed.info`` dict.

    Returns:
        Tuple of (raw amount string, decimals int).
    """
    token_amount = parsed_info.get("tokenAmount")
    if isinstance(token_amount, dict):
        amount = str(token_amount.get("amount") or parsed_info.get("amount") or "0")
        decimals = _to_int(token_amount.get("decimals"), 0)
        return amount, decimals
    return str(parsed_info.get("amount") or "0"), 0


def spl_transfer_row_fields(
    parsed_info: dict[str, Any],
    *,
    checked: bool,
) -> dict[str, Any] | None:
    """Extract (amount, mint, decimals) for an SPL transfer instruction.

    Args:
        parsed_info: The instruction's ``parsed.info`` dict.
        checked: True for ``transferChecked`` (carries mint + decimals).

    Returns:
        Dict with ``amount`` (int), ``mint`` (str), ``decimals`` (int),
        or None when the amount is not a positive value.
    """
    amount_str, decimals = _token_amount(parsed_info)
    amount = _to_int(amount_str, 0)
    if amount <= 0:
        return None
    mint = str(parsed_info.get("mint") or "")
    return {
        "amount": amount,
        "mint": mint,
        "decimals": decimals or _known_decimals(mint),
    }


__all__ = ["_token_amount", "spl_transfer_row_fields"]
