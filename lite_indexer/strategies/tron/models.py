"""
Tron Strategy - Constants and Configuration
============================================
Tracked TRC20 tokens, pagination limits, and block range caps.
"""

from __future__ import annotations

from typing import Any

# ── Tracked TRC20 Tokens ─────────────────────────────────────────────

TRC20_TOKENS: list[dict[str, Any]] = [
    {"address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "symbol": "USDT", "decimals": 6, "name": "Tether USD"},
    {"address": "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8", "symbol": "USDC", "decimals": 6, "name": "USD Coin"},
    {"address": "TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR", "symbol": "WTRX", "decimals": 6, "name": "Wrapped TRX"},
]

# Max events per page from TronGrid (200 is max)
TRON_PAGE_SIZE = 200
# Max block range per query (TronGrid limits)
TRON_MAX_BLOCK_RANGE = 2000
