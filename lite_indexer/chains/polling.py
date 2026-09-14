"""
Chain polling and health-check configuration.
"""

from __future__ import annotations

import os

# ── Health Check Configuration ─────────────────────────────────────

# How often (in seconds) to re-verify an endpoint after a successful check
ENDPOINT_VERIFY_INTERVAL = 3600  # 1 hour

# How long to wait for an endpoint verification response
ENDPOINT_VERIFY_TIMEOUT = 10.0

# Block range to use for endpoint health verification (small to avoid range errors)
VERIFY_BLOCK_DELTA = 100  # Check 100 blocks back from tip

# Max concurrent endpoint-verification PROBES across ALL chains during the
# startup pass. This is a GLOBAL in-flight bound (not just a chain count):
# verification used to be serialized inside each chain's sync loop, leaving
# most chains with an empty log pool for hours; hoisting it to a parallel
# pass needs a bound that is also MEMORY-safe (each probe may buffer a large
# eth_getLogs response). Six in-flight probes matches the previous steady
# state and stays well under the recycle ceiling.
ENDPOINT_VERIFY_CONCURRENCY: int = int(os.environ.get("ENDPOINT_VERIFY_CONCURRENCY", "6"))

# ── Chain-specific block time (seconds) for adaptive polling ───────────
CHAIN_POLL_INTERVALS: dict[str, float] = {
    "ethereum": 12.0,
    "base": 2.0,
    "bsc": 3.0,
    "polygon": 2.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "avalanche": 2.0,
    "fantom": 1.0,
    "linea": 12.0,
    "scroll": 3.0,
    "zksync": 1.0,
    "mantle": 2.0,
    "gnosis": 5.0,
    "celo": 5.0,
    "solana": 0.4,
    "tron": 3.0,
    "robinhood": 1.0,  # Arbitrum Orbit L2, ~0.25s block time — poll ~1s
}
