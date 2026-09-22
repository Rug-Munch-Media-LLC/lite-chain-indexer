"""pump.fun bonding-curve decoding and PDA derivation.

Extracted from ``launch.py`` (ratchet-down split). See that module's
docstring for the bonding-curve layout and REVIEW notes.
"""

from __future__ import annotations

from lite_indexer.logging import get_logger
from lite_indexer.strategies.solana.launch_constants import (
    BONDING_CURVE_DISCRIMINATOR,
    BONDING_CURVE_SEED,
    PUMP_FUN_PROGRAM,
)
from lite_indexer.strategies.solana.launch_models import CurveState
from lite_indexer.strategies.solana.launch_tx import _b58encode

logger = get_logger(__name__)


def decode_bonding_curve(data: bytes) -> CurveState | None:
    """Decode a pump.fun bonding-curve account (49-byte layout).

    Layout (spec): 8-byte discriminator ``0x17b7f83760d8ac60``, five
    little-endian u64s — virtualTokenReserves, virtualSolReserves,
    realTokenReserves, realSolReserves, tokenTotalSupply — then the
    ``complete`` flag byte. Price = virtualSolReserves /
    virtualTokenReserves (constant product over VIRTUAL reserves).

    [REVIEW] The spec names a 32-byte creator at offset 49–80 (an
    81-byte account). The real pump.fun ``BondingCurve`` struct is 49
    bytes with no creator field; this decoder extracts the creator only
    when the account is ≥81 bytes and leaves it '' for the standard
    49-byte account.

    Args:
        data: Raw account data bytes.

    Returns:
        :class:`CurveState`, or None when the data is not a valid
        bonding-curve account (wrong length or discriminator).
    """
    if len(data) < 49:
        return None
    if data[:8] != BONDING_CURVE_DISCRIMINATOR:
        logger.debug("solana_bonding_curve_bad_discriminator")
        return None
    virtual_token = int.from_bytes(data[8:16], "little")
    virtual_sol = int.from_bytes(data[16:24], "little")
    real_token = int.from_bytes(data[24:32], "little")
    real_sol = int.from_bytes(data[32:40], "little")
    total_supply = int.from_bytes(data[40:48], "little")
    complete = data[48] != 0
    creator = _b58encode(data[49:81]) if len(data) >= 81 else ""
    price_sol = (virtual_sol / virtual_token) if virtual_token else 0.0
    return CurveState(
        virtual_token_reserves=virtual_token,
        virtual_sol_reserves=virtual_sol,
        real_token_reserves=real_token,
        real_sol_reserves=real_sol,
        token_total_supply=total_supply,
        complete=complete,
        creator=creator,
        price_sol=price_sol,
    )


def bonding_curve_pda(mint: str) -> str | None:
    """Derive the pump.fun bonding-curve PDA address for a mint.

    PDA seeds are ``["bonding-curve", mint]`` under the pump.fun program
    (spec). Requires the optional ``solders`` package; returns None when
    it is unavailable so callers can fall back to RPC-side resolution.

    Args:
        mint: Token mint base58 address.

    Returns:
        Bonding-curve PDA base58 address, or None when solders is
        unavailable or the mint address is invalid.
    """
    try:
        from solders.pubkey import Pubkey
    except ImportError:  # pragma: no cover — environment dependent
        return None
    try:
        mint_pubkey = Pubkey.from_string(mint)
        program_id = Pubkey.from_string(PUMP_FUN_PROGRAM)
    except Exception:  # noqa: BLE001 — invalid address
        return None
    for bump in range(255, -1, -1):
        try:
            pda = Pubkey.create_program_address(
                [BONDING_CURVE_SEED, bytes(mint_pubkey), bytes([bump])],
                program_id,
            )
            return str(pda)
        except ValueError:
            continue
    return None
