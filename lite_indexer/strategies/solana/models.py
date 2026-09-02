"""
Solana Strategy - Constants and Configuration
==============================================
Tracked SPL tokens, RPC endpoints, and concurrency limits.
"""

from __future__ import annotations

from typing import Any

# ── Tracked SPL Tokens ───────────────────────────────────────────────
SOLANA_TRACKED_TOKENS: list[dict[str, Any]] = [
    {"mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "symbol": "USDC", "decimals": 6, "name": "USD Coin"},
    {"mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "symbol": "USDT", "decimals": 6, "name": "Tether USD"},
    {"mint": "So11111111111111111111111111111111111111112", "symbol": "SOL", "decimals": 9, "name": "Wrapped SOL"},
    {"mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "symbol": "BONK", "decimals": 5, "name": "Bonk"},
    {"mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN", "symbol": "JUP", "decimals": 6, "name": "Jupiter"},
    {"mint": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", "symbol": "POPCAT", "decimals": 9, "name": "Popcat"},
    {"mint": "ukHH6c7mMyiWCf1b9pnWe25TSpkDDt3H5pQZgZ74J82", "symbol": "BOME", "decimals": 6, "name": "BOOK OF MEME"},
    {"mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "symbol": "WIF", "decimals": 6, "name": "dogwifhat"},
    {"mint": "A3eME5CetyZPBoWbRUwY3tSe25S6tb18ba9ZPbWk9eFp", "symbol": "PENG", "decimals": 6, "name": "Peng"},
    {
        "mint": "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5",
        "symbol": "MEW",
        "decimals": 5,
        "name": "cat in a dogs world",
    },
]

# Maximum signatures to fetch per tracked token per cycle
MAX_SIGNATURES_PER_TOKEN = 100
# 2026-09-01 catch-up: max backward pages walked per mint per sync window
# (100 sigs/page x 40 pages = 4000 sigs deep — enough for a multi-day
# catch-up on a major mint without unbounded RPC spend).
SOLANA_SIG_MAX_PAGES = 40
# 2026-09-01 catch-up: windows deeper than this switch to block-based
# extraction (getBlock jsonParsed). ~2000 slots ≈ real-time polling depth.
SIG_WINDOW_MAX_SLOTS = 100
# Max slots processed per block-based catch-up sync (bounded chunks; the
# daemon advances the highwater per window and the next window continues).
SOLANA_BLOCK_MAX_PER_SYNC = 200

# Max concurrent getTransaction calls
SOLANA_TX_CONCURRENCY = 10

# ── Dynamic Token Discovery (solana_launches → tracked set) ──────────
# 2026-09-02: new meme tokens (pump.fun/Raydium launches) are written to
# wallet_memory.solana_launches by the launch detector every cycle. The
# strategy merges recent mints into its tracked set so their transfers
# are indexed like the hardcoded base tokens.
# Max new mints merged into the tracked set per sync cycle (RPC budget).
SOLANA_DISCOVERY_MAX_PER_CYCLE = 50
# Lookback window for launch discovery (hours).
SOLANA_DISCOVERY_LOOKBACK_HOURS = 24
# Hard cap on total tracked tokens (base + dynamic). Oldest dynamic
# tokens are evicted first; the hardcoded base set is never evicted.
SOLANA_MAX_TRACKED_TOKENS = 300

# ── Launchpad Block Watcher (pump.fun creates) ───────────────────────
# 2026-09-02: launch detection from tracked-mint txs alone cannot see
# brand-new pump.fun mints (their creation tx is never fetched), which
# starved solana_launches → discovery → deployer capture. The watcher
# scans the newest blocks (every tx) for token creations, bounded per
# cycle by the number of getBlock calls.
SOLANA_LAUNCHPAD_MAX_BLOCKS = 4
