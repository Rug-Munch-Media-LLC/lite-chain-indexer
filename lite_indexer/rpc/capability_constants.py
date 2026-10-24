"""Constants for endpoint capability probing.

Split out of capabilities.py (2026-10-24) so the probe modules can share them
without importing the facade. Names, values and comments are unchanged.
"""

from __future__ import annotations

# ── Constants ──────────────────────────────────────────────────────────
# Timeout for each individual capability probe call. Verification uses a
# separate, tighter liveness timeout; capability probes must stay well
# under the RPC call timeout so a slow endpoint cannot stall the
# verification cycle. Kept short (~3 s) because a capability verdict is
# reached from a couple of bounded calls, not an exhaustive search.
CAPABILITY_PROBE_TIMEOUT = 3.0

# Bounded eth_getLogs range probe (2026-09-14 latency fix). The probe used
# to binary-search the exact max range (up to 8 extra calls, each up to the
# probe timeout) — measured ~40 s per chain during startup on bsc. Instead
# it ascends a few candidate sizes and STOPS at the first failure, so the
# verdict is a conservative LOWER BOUND reached in at most
# ``GETLOGS_PROBE_MAX_CALLS`` round-trips at ``GETLOGS_PROBE_TIMEOUT`` each.
# Routing only needs a safe chunk size; the exact maximum does not matter.
GETLOGS_RANGE_LADDER: tuple[int, ...] = (10, 100, 1000, 10000)
GETLOGS_PROBE_CANDIDATES: tuple[int, ...] = (10, 100, 1000)
GETLOGS_PROBE_MAX_CALLS = 3
GETLOGS_PROBE_TIMEOUT = 2.5
#: Ceiling for a single ladder rung once its budget is scaled by span. The
#: 100-block rung needs 23-35 s on bsc endpoints that serve it; 30 s covers
#: the fast ones and still bounds a pathological startup to 3 rungs.
GETLOGS_PROBE_RUNG_TIMEOUT_CAP = 30.0

# Well-known, funded EOA (vitalik.eth) — a balance at genesis-era blocks
# only resolves on an archive-capable node; non-archive nodes error with
# a missing-trie-node / header-not-found response.
KNOWN_FUNDED_ADDRESS = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

# Standard ERC-20 Transfer topic — dense enough that a probe range is
# representative of real getLogs work while still returning fast.
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Seeded per-method compute-unit weights (provider-docs CU table, e.g.
# KeyedProvider-style pricing). eth_getBlockReceipts is not in most public CU
# tables — the ~50 estimate assumes ~5 receipt-equivalent units per
# returned receipt at typical block density; revisit when provider docs
# publish an authoritative figure.
SEEDED_CU_TARIFF: dict[str, float] = {
    "eth_chainId": 5.0,
    "eth_blockNumber": 5.0,
    "eth_getBalance": 10.0,
    "eth_getCode": 25.0,
    "eth_call": 26.0,
    "eth_getBlockByNumber": 16.0,
    "eth_getTransactionReceipt": 10.0,
    "eth_getLogs": 75.0,
    "eth_getBlockReceipts": 50.0,
}


# Verdict freshness: mirrors ENDPOINT_VERIFY_INTERVAL (defined here to avoid
# importing the chains package into this low-level module).
CAPABILITY_PERSIST_TTL = 3600.0


# How long a ``logs_capable=False`` verdict may keep an endpoint out of the
# desperation probe. A capability exclusion is a LOCALLY SYNTHESIZED verdict,
# not a provider ban, so it must never permanently remove endpoint capacity:
# a probe false-negative (e.g. a tip-range shape an endpoint only rejects for
# that shape) would otherwise freeze a chain whose whole log pool is
# capability-excluded. After this interval the verdict is treated as stale and
# the endpoint becomes probe-eligible again, so a chain self-heals in bounded
# time instead of waiting for the next hourly re-verification (or forever).
CAPABILITY_REPROBE_INTERVAL = 300.0
