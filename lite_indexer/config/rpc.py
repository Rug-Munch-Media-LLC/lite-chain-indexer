"""RPC configuration for LiteChainIndexer."""

from __future__ import annotations

import os

# Load the shared env file (same file the daemon's systemd unit sources
# via EnvironmentFile) BEFORE any os.environ reads below, so every entry
# point resolves the same ${ALCHEMY_KEY} / ${INFURA_KEY} / ${HELIUS_KEY} RPC
# placeholders. Idempotent and non-destructive (existing env wins).
from lite_indexer.config.indexing import DATA_DIR
from lite_indexer.env import load_env_file

load_env_file()

# RPC call timeout for eth_getLogs (longer since it can return large data)
RPC_TIMEOUT = 30

# Max backoff for recently-failed endpoints (seconds)
BACKOFF_CAP = 8

# Max wait for rate-limited endpoints (seconds)
RATE_LIMIT_WAIT_CAP = 16

# Default timeout for RPC calls
RPC_CALL_TIMEOUT = 15.0

# ── HTTP connection-pool ceilings (2026-09-13 socket leak) ───────────────
# Every long-lived httpx.AsyncClient must carry explicit limits so the
# process socket count stays bounded. The daemon was measured with 150
# open sockets before this was pinned down (default httpx pool is 100 per
# client). See lite_indexer/util/http.py.
HTTP_MAX_CONNECTIONS: int = int(os.environ.get("HTTP_MAX_CONNECTIONS", "20"))
HTTP_MAX_KEEPALIVE_CONNECTIONS: int = int(os.environ.get("HTTP_MAX_KEEPALIVE_CONNECTIONS", "10"))
HTTP_KEEPALIVE_EXPIRY: float = float(os.environ.get("HTTP_KEEPALIVE_EXPIRY", "30.0"))

# Max module-global pooled call clients in rpc/client.py keyed by timeout.
# The cache previously grew one AsyncClient per distinct timeout and never
# evicted or closed them (the primary socket leak).
RPC_CALL_CLIENTS_MAX: int = int(os.environ.get("RPC_CALL_CLIENTS_MAX", "4"))

# Timeout for block timestamp fetch calls
TIMESTAMP_FETCH_TIMEOUT = 15

# Exponential backoff retry for transient RPC failures (in RpcClient.call):
# attempts at 0s, 0.1s, 0.2s, 0.4s (base doubles each attempt, capped at 1s).
RPC_RETRY_ATTEMPTS = int(os.environ.get("RPC_RETRY_ATTEMPTS", "3"))
RPC_RETRY_BASE_DELAY_S = float(os.environ.get("RPC_RETRY_BASE_DELAY_S", "0.1"))

# Semaphore limit for parallel RPC calls
GET_LOGS_SEMAPHORE_LIMIT = 10
# ── Endpoint Health Tracking ──────────────────────────────────────────

ENDPOINT_COOLDOWN = 300
MAX_FAILURES = 3

# How often (in seconds) to re-verify an endpoint after a successful check
ENDPOINT_VERIFY_INTERVAL = 3600  # 1 hour

# How long to wait for an endpoint verification response
ENDPOINT_VERIFY_TIMEOUT = 10.0

# Block range to use for endpoint health verification (small to avoid range errors)
VERIFY_BLOCK_DELTA = 100  # Check 100 blocks back from tip
# ── Bloom Filter Pre-Filtering ────────────────────────────────────────
# Adopts the TrueBlocks approach: use each block header's built-in
# logsBloom to skip blocks that cannot contain a queried address/topic
# BEFORE issuing the (expensive) eth_getLogs call.
BLOOM_ENABLED: bool = os.environ.get("BLOOM_ENABLED", "1") != "0"

# Max bloom filters cached in memory (LRU). Each bloom is 256 bytes.
BLOOM_CACHE_MAX: int = 200_000

# How long a cached bloom filter is trusted (seconds) before refetch.
# Reduced 7d → 2d (2026-08-08): bloom filters are the largest key group in
# Redis (lci:bloom:*) and were filling 99% of memory. They are secondary to
# memory-LRU eviction — if they expire sooner they are rebuilt on next block.
BLOOM_CACHE_TTL: int = 2 * 24 * 3600  # 2 days

# Optional Redis bloom cache. When set (redis://...), blooms are also
# mirrored to Redis so multiple indexer processes share the cache.
REDIS_URL: str = os.environ.get("REDIS_URL", "")

# Max concurrent eth_getBlockByNumber header fetches for bloom warming.
BLOOM_FETCH_CONCURRENCY: int = 8
# ── Intelligent RPC Routing ───────────────────────────────────────────
# Per-method routing: eth_getLogs is heavy and free-tier friendly; go to
# public RPCs. Transaction receipts are small and latency-sensitive; go
# to dedicated (API-key) nodes first.
RPC_METHOD_ROUTING: dict[str, str] = {
    "eth_getLogs": "public",
    "eth_blockNumber": "public",
    "eth_getBlockByNumber": "public",
    "eth_getTransactionReceipt": "dedicated",
    "eth_call": "dedicated",
}

# Circuit breaker per RPC endpoint: after N failures inside a window the
# endpoint is skipped for COOLDOWN seconds, then half-open probes resume.
# Note: 429 rate limits do NOT count toward the threshold — they only
# trigger backoff (see rpc/pool.record_rate_limit). Threshold raised
# 5->10 and window 60->120 (2026-08-07) so transient public-tier throttling
# does not trip the breaker on busy chains.
CIRCUIT_FAILURE_THRESHOLD: int = 10
CIRCUIT_WINDOW_SECONDS: int = 120
CIRCUIT_COOLDOWN_SECONDS: int = 30

# Desperation probe: when EVERY endpoint in a chain's pool is open/banned
# (pool-wide outage), the pool stops hammering all endpoints and instead
# force-probes the least-banned endpoint at most once per this interval.
# This lets the chain self-heal after a mass outage without a 30s+ wait
# per ban window and without racing the ban timer.
CIRCUIT_DESPERATION_PROBE_INTERVAL: float = float(os.environ.get("CIRCUIT_DESPERATION_PROBE_INTERVAL", "30.0"))

# RPC usage/cost tracking: keep a bounded per-endpoint usage ledger.
RPC_USAGE_MAX_ENDPOINTS: int = 512
# ── Intelligent RPC Throttling (rpc/throttle.py) ─────────────────────
# Rate-limit header tracking + pre-emptive throttling layered on top of
# the per-provider token buckets (rpc/ratelimit.py). The tracker honors
# X-RateLimit-Remaining / X-RateLimit-Limit / Retry-After headers exactly
# and persists all state to RPC_THROTTLE_FILE so a restart mid-429-window
# does not re-hammer the endpoint that just told us to back off.

# Persisted tracker state path (default: DATA_DIR/rpc_throttle.json).
RPC_THROTTLE_FILE: str = os.environ.get("RPC_THROTTLE_FILE", str(DATA_DIR / "rpc_throttle.json"))

# Pre-emptive throttle threshold: when an endpoint advertises a budget via
# X-RateLimit-Limit/Remaining and we have consumed more than this percent,
# the next request is delayed to stay under the limit instead of waiting
# for a 429. 80 = slow down after consuming 80% of the budget.
RPC_PREEMPTIVE_THRESHOLD_PCT: float = float(os.environ.get("RPC_PREEMPTIVE_THRESHOLD_PCT", "80.0"))

# Per-method request budgets (req/s) used when the endpoint does NOT
# advertise headers. eth_getLogs is the expensive call on free tiers
# (3-10 req/s typical) while eth_blockNumber is nearly free — cheap
# calls must never starve expensive ones.
RPC_METHOD_REQ_PER_SEC: dict[str, float] = {
    "eth_getLogs": 3.0,
    "eth_traceTransaction": 3.0,
    "trace_block": 3.0,
    "trace_filter": 3.0,
    "trace_transaction": 3.0,
    "eth_call": 5.0,
    "eth_getTransactionReceipt": 10.0,
    "eth_getBlockByNumber": 10.0,
    "eth_blockNumber": 20.0,
    "default": 10.0,
}

# Default per-method budget used when a method has no explicit entry.
RPC_METHOD_DEFAULT_RATE: float = float(os.environ.get("RPC_METHOD_DEFAULT_RATE", "10.0"))

# Hard cap (seconds) on the pre-emptive/adaptive sleep introduced by the
# throttle tracker. Retry-After from a real 429 may exceed this and is
# honored exactly; this only bounds the speculative pre-emptive sleep.
RPC_THROTTLE_MAX_WAIT: float = float(os.environ.get("RPC_THROTTLE_MAX_WAIT", "30.0"))

# RPC auto-discovery: how often (seconds) to scan chainlist.org for new
# free endpoints. 0 disables. Default: daily.
RPC_DISCOVERY_INTERVAL: int = int(os.environ.get("RPC_DISCOVERY_INTERVAL", "86400"))
# ── Endpoint Ban (403/429 hard ban) ─────────────────────────────────────
# Endpoints returning 403/429 are rate-limited or blacklisted by the
# provider for a while; hammering them only deepens the ban. These are
# banned for an exponentially growing duration (base * 2^ban_count,
# capped) instead of the plain 300s cooldown used for generic failures.
# Cap reduced 3600 -> 300 (2026-09-02): public free RPCs reset rate limits
# every minute; a 1h cap wasted recovery time during the EVM dex_events
# staleness crisis (517 rate-limit events/hour, 190h+ lag).
ENDPOINT_BAN_BASE_SECONDS: int = int(os.environ.get("ENDPOINT_BAN_BASE_SECONDS", "60"))
ENDPOINT_BAN_CAP_SECONDS: int = int(os.environ.get("ENDPOINT_BAN_CAP_SECONDS", "300"))
# ── On-Demand Historical RPC Queries (lite_indexer.query) ───────────────
# The "no-full-indexing-needed" architecture: the QueryRouter answers
# queries about ANY block/tx/address historically by routing to the best
# data source — ClickHouse (indexed, hot) → B2 parquet (cold) → live RPC
# (on-demand). The system never says "data not available": when the
# requested range is not indexed, it dials the chain's RPC pool for the
# exact block/tx in question (bounded, rate-limit-protected, cached).
ON_DEMAND_RPC_ENABLED: bool = os.environ.get("ON_DEMAND_RPC_ENABLED", "1") != "0"

# How long (seconds) fetched RPC results are cached before refetching.
# Keeps repeated queries for the same tx/address/block off the RPC tier.
ON_DEMAND_RPC_CACHE_TTL: int = int(os.environ.get("ON_DEMAND_RPC_CACHE_TTL", "3600"))

# Max block range for a single on-demand RPC query. Address/transfer
# queries beyond this are clamped to the newest window (tip - range) so a
# single request can never blow a free-tier endpoint's range cap.
ON_DEMAND_RPC_MAX_BLOCK_RANGE: int = int(os.environ.get("ON_DEMAND_RPC_MAX_BLOCK_RANGE", "5000"))

# Max consecutive on-demand RPC calls before pausing (rate-limit
# protection). Free tiers throttle bursts; after this many calls the
# fetcher sleeps ON_DEMAND_RPC_PAUSE_SECONDS, then resumes counting.
ON_DEMAND_RPC_MAX_CONSECUTIVE: int = int(os.environ.get("ON_DEMAND_RPC_MAX_CONSECUTIVE", "10"))

# Pause duration (seconds) after ON_DEMAND_RPC_MAX_CONSECUTIVE calls.
ON_DEMAND_RPC_PAUSE_SECONDS: float = float(os.environ.get("ON_DEMAND_RPC_PAUSE_SECONDS", "1.0"))

# Max entries in the on-demand RPC result cache (bounds memory).
ON_DEMAND_RPC_CACHE_MAX_ENTRIES: int = int(os.environ.get("ON_DEMAND_RPC_CACHE_MAX_ENTRIES", "2048"))

# ── Solana Address Backfill (free getSignaturesForAddress parity) ──────
# On-demand history recovery for a single address. Public RPC first, then
# Alchemy, then Helius (all free tiers, keys via env). The first successful
# provider answers; a provider error falls through to the next. Results are
# indexed into wallet_memory.address_activity so the address is served from
# ClickHouse forever after (see ADDRESS_BACKFILL_ENABLED + the guard table).
ADDRESS_BACKFILL_ENABLED: bool = os.environ.get("ADDRESS_BACKFILL_ENABLED", "1") != "0"

# Max signatures pulled per address (one-time, then never re-fetched).
ADDRESS_BACKFILL_MAX_SIGNATURES: int = int(os.environ.get("ADDRESS_BACKFILL_MAX_SIGNATURES", "1000"))

# Max transactions fetched+parsed per backfill (bounds free-tier spend).
ADDRESS_BACKFILL_MAX_TX: int = int(os.environ.get("ADDRESS_BACKFILL_MAX_TX", "250"))

# Smaller cap for the lazy API path so a first-touch request stays bounded.
ADDRESS_BACKFILL_LAZY_MAX_TX: int = int(os.environ.get("ADDRESS_BACKFILL_LAZY_MAX_TX", "50"))

# getSignaturesForAddress page size (provider max is 1000; 100 is kind).
ADDRESS_BACKFILL_PAGE_SIZE: int = int(os.environ.get("ADDRESS_BACKFILL_PAGE_SIZE", "100"))

# Concurrent getTransaction calls during a backfill (free-tier friendly).
ADDRESS_BACKFILL_CONCURRENCY: int = int(os.environ.get("ADDRESS_BACKFILL_CONCURRENCY", "4"))

# Per-RPC timeout (seconds) for backfill calls.
ADDRESS_BACKFILL_TIMEOUT: float = float(os.environ.get("ADDRESS_BACKFILL_TIMEOUT", "20.0"))

# In-process negative cache: after a failed/unwritable attempt, don't retry
# the same address for this many seconds (prevents read-only API processes
# from burning free-tier RPC on every request).
ADDRESS_BACKFILL_ATTEMPT_TTL: float = float(os.environ.get("ADDRESS_BACKFILL_ATTEMPT_TTL", "300.0"))

# Guard table recording which (chain, address) pairs are already indexed.
CH_ADDRESS_BACKFILL_STATE_TABLE: str = (
    os.environ.get("CH_ADDRESS_BACKFILL_STATE_TABLE") or "wallet_memory.address_backfill_state"
)

# Multi-provider fallback endpoints. Public first, then keyed free tiers.
# Keyed templates keep ${ENV_VAR} placeholders — never a real key in code;
# unresolved templates are dropped from the provider list at import.
SOLANA_PUBLIC_RPC: str = os.environ.get("SOLANA_PUBLIC_RPC", "https://api.mainnet-beta.solana.com")
SOLANA_PUBLIC_RPC_FALLBACK: str = os.environ.get("SOLANA_PUBLIC_RPC_FALLBACK", "https://solana-rpc.publicnode.com")
SOLANA_ALCHEMY_RPC_TEMPLATE: str = os.environ.get(
    "SOLANA_ALCHEMY_RPC_TEMPLATE", "https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}"
)
SOLANA_HELIUS_RPC_TEMPLATE: str = os.environ.get(
    "SOLANA_HELIUS_RPC_TEMPLATE", "https://mainnet.helius-rpc.com/?api-key=${HELIUS_KEY}"
)

# ── Solana Real-Time Websocket Ingestion (logsSubscribe) ───────────────
# A FREE real-time feed: subscribe to Solana RPC logsSubscribe for a
# watchlist of program ids (and/or accountSubscribe for a watchlist of
# accounts), fetch each notified transaction, and index it into
# address_activity. Watchlist/program-scoped on purpose — never a
# full-fidelity index of every transaction on the chain.
SOLANA_WS_ENABLED: bool = os.environ.get("SOLANA_WS_ENABLED", "0") != "0"
SOLANA_WS_URL: str = os.environ.get("SOLANA_WS_URL", "wss://api.mainnet-beta.solana.com")
# Comma-separated program ids for logsSubscribe (mentions filter).
SOLANA_WS_PROGRAMS: tuple[str, ...] = tuple(
    p.strip() for p in os.environ.get("SOLANA_WS_PROGRAMS", "").split(",") if p.strip()
)
# Comma-separated account addresses for accountSubscribe (watchlist).
SOLANA_WS_ACCOUNTS: tuple[str, ...] = tuple(
    a.strip() for a in os.environ.get("SOLANA_WS_ACCOUNTS", "").split(",") if a.strip()
)
SOLANA_WS_COMMITMENT: str = os.environ.get("SOLANA_WS_COMMITMENT", "finalized")
SOLANA_WS_PING_INTERVAL: float = float(os.environ.get("SOLANA_WS_PING_INTERVAL", "30.0"))
SOLANA_WS_RECONNECT_BASE_SECONDS: float = float(os.environ.get("SOLANA_WS_RECONNECT_BASE_SECONDS", "1.0"))
SOLANA_WS_RECONNECT_MAX_SECONDS: float = float(os.environ.get("SOLANA_WS_RECONNECT_MAX_SECONDS", "60.0"))
# Max reconnects before giving up; 0 = retry forever.
SOLANA_WS_MAX_RECONNECTS: int = int(os.environ.get("SOLANA_WS_MAX_RECONNECTS", "0"))
