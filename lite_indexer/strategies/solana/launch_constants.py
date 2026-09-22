"""Solana launch venue constants — programs, discriminators, sinks.

Extracted from ``launch.py`` (ratchet-down split). See that module's
docstring for the full venue/signal model and confidence semantics.
"""

from __future__ import annotations

import os

# ── Programs / Accounts (mainnet, exact — from task spec) ─────────────

# pump.fun launchpad program.
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
# pump.fun `create` instruction discriminator = sha256("global:create")[:8]
# (big-endian), i.e. bytes 0x18 0x1e 0xc8 0x28 0x05 0x1c 0x07 0x77.
PUMP_FUN_CREATE_DISCRIMINATOR = bytes([24, 30, 200, 40, 5, 28, 7, 119])

# PumpSwap AMM — `create_pool` is the bonding-curve graduation.
PUMP_SWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# Raydium AMM v4 (`initialize2`), CPMM (`initialize`/`createPool`), CLMM.
RAYDIUM_AMM_V4_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM_PROGRAM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
RAYDIUM_CLMM_PROGRAM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"

# LaunchLab launchpad (`initialize_v2`).
LAUNCHLAB_PROGRAM = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
# LetsBonk platform config account — when present on a LaunchLab
# initialize_v2, the launch is a Bonk.fun launch.
LETSBONK_PLATFORM_CONFIG = "FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1"

# ── Bonding curve ─────────────────────────────────────────────────────

# pump.fun bonding-curve account discriminator (8-byte, big-endian).
BONDING_CURVE_DISCRIMINATOR = bytes.fromhex("17b7f83760d8ac60")
# PDA seed prefix: find_program_address(["bonding-curve", mint], pump.fun).
BONDING_CURVE_SEED = b"bonding-curve"

# ── Persistence / Streaming ───────────────────────────────────────────

# ClickHouse table for Solana launch events. Override with
# CH_SOLANA_LAUNCHES_TABLE for operators who prefer a different database.
SOLANA_LAUNCHES_TABLE: str = os.environ.get(
    "CH_SOLANA_LAUNCHES_TABLE", "wallet_memory.solana_launches"
)

# NATS routing for the launch stream (subject is chain.solana.launch).
# Mirrors lite_indexer.strategies.tron.launch: event type "launch" maps to
# the shared "launch": "chain.{chain}.launch" subject template.
NATS_EVENT_TYPE = "launch"
NATS_SUBJECT = "chain.solana.launch"
