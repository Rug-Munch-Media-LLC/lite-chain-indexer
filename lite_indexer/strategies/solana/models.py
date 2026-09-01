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
SIG_WINDOW_MAX_SLOTS = 2000
# Max slots processed per block-based catch-up sync (bounded chunks; the
# daemon advances the highwater per window and the next window continues).
SOLANA_BLOCK_MAX_PER_SYNC = 500

# Max concurrent getTransaction calls
SOLANA_TX_CONCURRENCY = 10
