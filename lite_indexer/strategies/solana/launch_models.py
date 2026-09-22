"""Solana launch event models and venue specs.

Extracted from ``launch.py`` (ratchet-down split). See that module's
docstring for the full venue/signal model and confidence semantics.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel

from lite_indexer.strategies.solana.launch_constants import (
    LAUNCHLAB_PROGRAM,
    LETSBONK_PLATFORM_CONFIG,
    PUMP_FUN_CREATE_DISCRIMINATOR,
    PUMP_FUN_PROGRAM,
    PUMP_SWAP_PROGRAM,
    RAYDIUM_AMM_V4_PROGRAM,
    RAYDIUM_CLMM_PROGRAM,
    RAYDIUM_CPMM_PROGRAM,
)


class LaunchEvent(BaseModel):
    """A Solana token-launch or graduation event.

    One event per transaction. ``launch_signal`` is True when the tx
    created the token (pump.fun create / Bonk.fun / LaunchLab
    initialize_v2); ``graduation_signal`` is True when the tx moved the
    token onto an AMM (PumpSwap create_pool, Raydium pool creation).
    A single tx (e.g. the pump.fun migration tx) can set both.

    ``mint`` is the new token (``meta.postTokenBalances[0].mint``) and
    ``creator`` the transaction fee payer (``accountKeys[0]``).
    """

    type: str = "solana_launch"
    chain: str = "solana"
    signature: str = ""
    slot: int = 0
    block_timestamp: str = ""  # ISO-8601 UTC
    mint: str = ""
    creator: str = ""
    venue: str = ""
    launch_signal: bool = False
    graduation_signal: bool = False
    confidence: str = "low"  # high (exact) | medium (name match) | low (program presence)
    instruction_name: str = ""
    source: str = "lite_indexer"
    ingested_at: str = ""


class CurveState(BaseModel):
    """Decoded pump.fun bonding-curve account state (49-byte layout).

    All reserve/supply values are raw token/SOL integer units; convert
    with the token decimals for display. ``price_sol`` is the constant-
    product price over the VIRTUAL reserves (SOL per token).
    """

    virtual_token_reserves: int = 0
    virtual_sol_reserves: int = 0
    real_token_reserves: int = 0
    real_sol_reserves: int = 0
    token_total_supply: int = 0
    complete: bool = False
    creator: str = ""  # base58 pubkey (only when the account is ≥81 bytes)
    price_sol: float = 0.0


@dataclass(frozen=True)
class VenueSpec:
    """Matching rules for one launch venue.

    Attributes:
        venue: Venue key stored in ``LaunchEvent.venue``.
        program: Program ID that owns the launch instruction.
        launch_instruction_names: Parsed instruction names treated as a
            launch signal (only fires for RPC-parsed programs).
        launch_discriminators: Exact 8-byte discriminators treated as a
            launch signal (raw instruction data prefix).
        requires_config_account: When set, program presence plus this
            account in the instruction (or anywhere in the tx account
            keys) is an exact launch match (LetsBonk / Bonk.fun).
        graduation: When True, program presence alone is a graduation
            signal (PumpSwap create_pool — see module docstring REVIEW).
    """

    venue: str
    program: str
    launch_instruction_names: frozenset[str] = frozenset()
    launch_discriminators: tuple[bytes, ...] = ()
    requires_config_account: str | None = None
    graduation: bool = False


# Venue priority order — when a tx matches several venues, the earliest
# entry wins as the primary venue (launchpad creates first, then
# bonding-curve graduation, then AMM listings).
VENUES: tuple[VenueSpec, ...] = (
    VenueSpec("pumpfun", PUMP_FUN_PROGRAM, launch_discriminators=(PUMP_FUN_CREATE_DISCRIMINATOR,)),
    VenueSpec(
        "bonkfun",
        LAUNCHLAB_PROGRAM,
        launch_instruction_names=frozenset({"initialize_v2"}),
        requires_config_account=LETSBONK_PLATFORM_CONFIG,
    ),
    VenueSpec(
        "launchlab", LAUNCHLAB_PROGRAM, launch_instruction_names=frozenset({"initialize_v2"})
    ),
    VenueSpec("pumpswap", PUMP_SWAP_PROGRAM, graduation=True),
    VenueSpec(
        "raydium_amm_v4",
        RAYDIUM_AMM_V4_PROGRAM,
        launch_instruction_names=frozenset({"initialize2"}),
    ),
    VenueSpec(
        "raydium_cpmm",
        RAYDIUM_CPMM_PROGRAM,
        launch_instruction_names=frozenset({"initialize", "createPool"}),
    ),
    VenueSpec(
        "raydium_clmm", RAYDIUM_CLMM_PROGRAM, launch_instruction_names=frozenset({"initialize"})
    ),
)

_VENUE_PRIORITY: dict[str, int] = {spec.venue: i for i, spec in enumerate(VENUES)}


@dataclass(frozen=True)
class _Match:
    """An instruction matched one venue."""

    venue: str
    launch_signal: bool
    graduation_signal: bool
    confidence: str
    instruction_name: str = ""
