"""
Solana token-launch detection — pump.fun / Bonk.fun / AMM pool creations.

Detects meme-coin launch lifecycle events from the SAME ``getTransaction``
(jsonParsed) payloads the Solana strategy already fetches for SPL balance
changes — zero extra RPC cost. Mirrors ``lite_indexer.strategies.tron.launch``
but for the Solana instruction model (program ID + discriminator / config
account, instead of event logs).

Venues and their launch signals:

  =================  =====================================================
  venue              signal
  =================  =====================================================
  ``pumpfun``        pump.fun program + 8-byte create discriminator
                     (exact: ``sha256("global:create")[:8]``)
  ``bonkfun``        LaunchLab program + LetsBonk platform config account
                     present in the instruction (initialize_v2 = Bonk.fun)
  ``launchlab``      LaunchLab program + parsed ``initialize_v2`` name
                     (no Bonk config account)
  ``pumpswap``       PumpSwap program presence — ``create_pool`` is the
                     bonding-curve graduation signal ([REVIEW] the
                     create_pool discriminator is not in the spec, so this
                     matches every PumpSwap instruction, swaps included —
                     confidence ``low``)
  ``raydium_amm_v4`` Raydium AMM v4 + parsed ``initialize2`` name
  ``raydium_cpmm``   Raydium CPMM + parsed ``initialize``/``createPool``
  ``raydium_clmm``   Raydium CLMM + parsed ``initialize`` name
  =================  =====================================================

[REVIEW] The stock Solana RPC parses only a fixed program set (system,
vote, stake, token, AToken, memo, token-2022, name-service, secp, LUT).
pump.fun, PumpSwap, Raydium and LaunchLab instructions therefore arrive as
raw base58 ``data``, and the venues above that rely on parsed instruction
names (launchlab, raydium_*) will not match until their exact 8-byte
discriminators are added to :data:`VENUES` — the spec provided only the
pump.fun create discriminator. ``pumpfun`` (exact discriminator) and
``bonkfun`` (exact config account) work out of the box.

One event per transaction:

  * ``launch_signal``     — True when the tx created the token (launchpad
    ``create`` / ``initialize_v2``).
  * ``graduation_signal`` — True when the tx moved the token onto an AMM
    (PumpSwap ``create_pool``, Raydium pool creation). A single tx can set
    both (the pump.fun migration tx carries create + create_pool).

The new token mint is ``meta.postTokenBalances[0].mint`` (spec) and the
creator is the transaction fee payer (``accountKeys[0]`` — same convention
as the strategy's deployer capture).

Bonding-curve layout (spec): the pump.fun curve account is 49 bytes —
8-byte discriminator ``0x17b7f83760d8ac60``, five little-endian u64s
(virtualTokenReserves, virtualSolReserves, realTokenReserves,
realSolReserves, tokenTotalSupply), then the ``complete`` flag byte.
Price = virtualSolReserves / virtualTokenReserves (constant product over
VIRTUAL reserves). [REVIEW] The spec also names a 32-byte creator at
offset 49–80, which requires an 81-byte account; the real pump.fun
``BondingCurve`` struct is 49 bytes with no creator field, so
:func:`decode_bonding_curve` decodes the 49-byte layout and only extracts
the creator when the account is ≥81 bytes.

The PDA is ``find_program_address(["bonding-curve", mint], pump_fun_program)``
(see :func:`bonding_curve_pda`, requires the optional ``solders`` package).

Integration point: in ``SolanaStrategy._fetch_and_parse`` (strategy.py),
after ``parse_solana_transfer_events``, call ``detect_launch(tx_data)`` and
buffer hits (e.g. ``self._last_launch_events``, mirroring
``_last_deployer_events``); then flush with ``await write_launch(evt,
ch_writer, nats_publisher)`` from the indexer's sync cycle where both the
ClickHouse writer and the NATS publisher are available.
"""


from lite_indexer.strategies.solana.launch_constants import (
    BONDING_CURVE_DISCRIMINATOR,
    BONDING_CURVE_SEED,
    LAUNCHLAB_PROGRAM,
    LETSBONK_PLATFORM_CONFIG,
    NATS_EVENT_TYPE,
    NATS_SUBJECT,
    PUMP_FUN_CREATE_DISCRIMINATOR,
    PUMP_FUN_PROGRAM,
    PUMP_SWAP_PROGRAM,
    RAYDIUM_AMM_V4_PROGRAM,
    RAYDIUM_CLMM_PROGRAM,
    RAYDIUM_CPMM_PROGRAM,
    SOLANA_LAUNCHES_TABLE,
)
from lite_indexer.strategies.solana.launch_curve import bonding_curve_pda, decode_bonding_curve
from lite_indexer.strategies.solana.launch_detect import detect_launch
from lite_indexer.strategies.solana.launch_models import VENUES, CurveState, LaunchEvent, VenueSpec
from lite_indexer.strategies.solana.launch_persist import (
    ensure_solana_launches_table,
    write_launch,
)
from lite_indexer.strategies.solana.launch_tx import _b58decode, _b58encode  # noqa: F401 — test surface

__all__ = [
    "PUMP_FUN_PROGRAM",
    "PUMP_FUN_CREATE_DISCRIMINATOR",
    "PUMP_SWAP_PROGRAM",
    "RAYDIUM_AMM_V4_PROGRAM",
    "RAYDIUM_CPMM_PROGRAM",
    "RAYDIUM_CLMM_PROGRAM",
    "LAUNCHLAB_PROGRAM",
    "LETSBONK_PLATFORM_CONFIG",
    "BONDING_CURVE_DISCRIMINATOR",
    "BONDING_CURVE_SEED",
    "SOLANA_LAUNCHES_TABLE",
    "NATS_EVENT_TYPE",
    "NATS_SUBJECT",
    "LaunchEvent",
    "CurveState",
    "VenueSpec",
    "VENUES",
    "detect_launch",
    "decode_bonding_curve",
    "bonding_curve_pda",
    "ensure_solana_launches_table",
    "write_launch",
]
