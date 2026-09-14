"""RPC configuration for LiteChainIndexer."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

# Load the shared env file (same file the daemon's systemd unit sources
# via EnvironmentFile) BEFORE any os.environ reads below, so every entry
# point resolves the same ${KEYED_RPC_KEY} / ${KEYED_RPC_KEY_2} / ${SOLANA_PROVIDER_KEY} RPC
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
# Upper bound for a jittered transient retry delay (seconds). Full jitter is
# uniform(0, min(cap, base * 2**attempt)); Retry-After windows stay exact.
RPC_RETRY_MAX_DELAY_S = float(os.environ.get("RPC_RETRY_MAX_DELAY_S", "1.0"))

# ── Backoff jitter (rpc/backoff.py) ──────────────────────────────────
# AWS-style FULL jitter de-synchronizes multi-worker retries so a recovering
# provider is not slammed by every worker at the same instant. Set
# RPC_BACKOFF_JITTER=0 to restore deterministic retries (diagnostics only).
# RPC_BACKOFF_JITTER_SEED makes the jitter RNG reproducible for tests.
RPC_BACKOFF_JITTER: bool = os.environ.get("RPC_BACKOFF_JITTER", "1") != "0"
RPC_BACKOFF_JITTER_SEED: str = os.environ.get("RPC_BACKOFF_JITTER_SEED", "")

# ── In-call eth_getLogs range splitting (rpc/range_split.py) ─────────
# When a provider rejects a getLogs range as too large, split [from,to] at
# the midpoint and retry both halves IN-LINE (Ponder-style) so the call
# still succeeds this cycle instead of waiting for a cross-cycle halving.
# Bounds keep the recursion finite: at most GETLOGS_SPLIT_MAX_DEPTH levels
# and never below GETLOGS_SPLIT_MIN_BLOCKS blocks per leaf.
GETLOGS_SPLIT_MAX_DEPTH: int = int(os.environ.get("GETLOGS_SPLIT_MAX_DEPTH", "8"))
GETLOGS_SPLIT_MIN_BLOCKS: int = int(os.environ.get("GETLOGS_SPLIT_MIN_BLOCKS", "1"))

# ── Cross-provider JSON-RPC batching (rpc/client.py call_batch) ──────
# Free RPCs rate-limit per REQUEST, so collapsing N independent same-shape
# calls into one JSON-RPC array POST multiplies throughput without extra
# requests. A batch counts as ONE HTTP request against the per-endpoint /
# per-account request caps (elements are still cost-weighted in the ledger).
# RPC_BATCH_MAX bounds the array size; RPC_BATCH_ENABLED=0 disables batching
# (every call transparently degrades to individual requests).
RPC_BATCH_ENABLED: bool = os.environ.get("RPC_BATCH_ENABLED", "1") != "0"
RPC_BATCH_MAX: int = int(os.environ.get("RPC_BATCH_MAX", "20"))

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

# ── Persistent RPC response cache (rpc/cache_db.py + rpc/respcache.py) ──
# L1 is the in-process LRU; L2 is a SQLite (WAL) store that survives restarts
# so backfill/reconcile never re-fetch immutable history. Every knob is
# failure-safe: a broken/oversized DB degrades to an L1-only cache.
RPC_CACHE_ENABLED: bool = os.environ.get("RPC_CACHE_ENABLED", "1") != "0"
# Persistent store path (default: DATA_DIR/rpc_cache.sqlite).
RPC_CACHE_DB: str = os.environ.get("LITE_INDEXER_CACHE_DB", str(DATA_DIR / "rpc_cache.sqlite"))
# Max cached entries (L2); oldest-accessed are evicted past this.
RPC_CACHE_MAX_ENTRIES: int = int(os.environ.get("RPC_CACHE_MAX_ENTRIES", "200000"))
# Entry TTL in seconds (default 7 days).
RPC_CACHE_TTL_S: float = float(os.environ.get("RPC_CACHE_TTL_S", str(7 * 24 * 3600)))
# Cap the DB size in MB (oldest entries evicted first); 0 disables the cap.
RPC_CACHE_MAX_MB: int = int(os.environ.get("RPC_CACHE_MAX_MB", "512"))
# Blocks closer than this to the chain tip are mutable (reorg window) and
# are never cached; historical reads below it are immutable.
RPC_CACHE_CONFIRMATIONS: int = int(os.environ.get("RPC_CACHE_CONFIRMATIONS", "64"))

# ── Stale-tip eviction (rpc/routing/health.py) ─────────────────────────
# An endpoint whose last reported height (eth_blockNumber / head response)
# lags the pool's max/median by more than this many blocks is demoted out
# of tip selection until it catches up. Catches endpoints that accept
# connections and answer calls but are stuck on an old fork/height.
RPC_STALE_TIP_BLOCKS: int = int(os.environ.get("RPC_STALE_TIP_BLOCKS", "5"))

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

# ── Per-account (provider) rate caps (rpc/provider_caps.py) ───────────
# ADDITIVE to the per-endpoint throttle: providers ban the ACCOUNT when
# many of their endpoints are hit at once across chains, so concurrency and
# requests/sec are capped per `provider_id_for_endpoint` (KeyedProvider account-
# wide, etc.). Callers queue when the budget is exhausted rather than fire.
RPC_PROVIDER_MAX_CONCURRENCY: int = int(os.environ.get("RPC_PROVIDER_MAX_CONCURRENCY", "4"))
RPC_PROVIDER_RPS: float = float(os.environ.get("RPC_PROVIDER_RPS", "10.0"))

# ── Per-provider / per-endpoint request budgets (rpc/budgets.py) ───────
# Attach a concrete (rps, max_concurrency, optional daily budget) to a
# provider ACCOUNT (registrable domain + key id) or to a single endpoint
# URL. The load-distribution defect this fixes: selection kept favouring
# the lowest-latency endpoint, so the same handful of free endpoints blew
# their per-IP request caps and got soft-banned while 30+ other endpoints
# sat idle. A per-identity budget — and a PRE-EMPTIVE cool-down at
# ``RPC_BUDGET_PREEMPTIVE_PCT`` of the daily budget, i.e. *before* the
# provider's 429/softban arrives — spreads the load instead of waiting for
# eviction.
#
# The public tree ships only the generic mechanism plus generic keyless
# defaults. Concrete free-tier values are supplied by the operator from
# the private overlay/env as JSON, never baked into git::
#
#   RPC_PROVIDER_BUDGETS='{"<provider-id>": {"rps": 5, "max_concurrency": 2,
#                                            "daily_budget": 100000}}'
#   RPC_ENDPOINT_BUDGETS='{"https://<host>/<path>": {"rps": 3,
#                                                    "daily_budget": 20000}}'
#
# Every field is optional; a missing field falls back to the generic default
# below (and finally to RPC_PROVIDER_RPS / RPC_PROVIDER_MAX_CONCURRENCY).
# ``daily_budget`` is a request count per UTC day (0 = unlimited).
# An endpoint entry wins over a provider entry; both win over the default.
RPC_PROVIDER_DAILY_BUDGET: int = int(os.environ.get("RPC_PROVIDER_DAILY_BUDGET", "0"))
RPC_BUDGET_PREEMPTIVE_PCT: float = float(os.environ.get("RPC_BUDGET_PREEMPTIVE_PCT", str(RPC_PREEMPTIVE_THRESHOLD_PCT)))
# Base cool-down applied when a budget hits the pre-emptive threshold; each
# repeat multiplies by RPC_BUDGET_COOLDOWN_BACKOFF, capped (then recovers —
# see rpc/budgets.py record_success / UTC day rollover).
RPC_BUDGET_COOLDOWN_SECONDS: float = float(os.environ.get("RPC_BUDGET_COOLDOWN_SECONDS", "120.0"))
RPC_BUDGET_COOLDOWN_BACKOFF: float = float(os.environ.get("RPC_BUDGET_COOLDOWN_BACKOFF", "2.0"))
RPC_BUDGET_COOLDOWN_MAX_SECONDS: float = float(os.environ.get("RPC_BUDGET_COOLDOWN_MAX_SECONDS", "1800.0"))
# Hard cap on the speculative per-budget pacing sleep (never blocks a call
# longer than the shared throttle cap; Retry-After still wins where relevant).
RPC_BUDGET_MAX_WAIT: float = float(os.environ.get("RPC_BUDGET_MAX_WAIT", str(RPC_THROTTLE_MAX_WAIT)))


@dataclass(frozen=True)
class ProviderBudget:
    """A request budget attached to a provider account or one endpoint.

    Attributes:
        rps: Requests/sec ceiling (0 = unlimited).
        max_concurrency: Max concurrent in-flight requests (0 = unlimited).
        daily_budget: Request count per UTC day (0 = unlimited).
        preemptive_pct: Cool the identity once this percent of
            ``daily_budget`` is consumed (default 80 = before the 429).
        explicit: True when the value came from an operator-supplied map
            rather than the generic keyless default. Only explicit (or
            daily-budgeted) identities are actively enforced here — the
            generic path keeps deferring to the existing provider caps.
    """

    rps: float
    max_concurrency: int
    daily_budget: int = 0
    preemptive_pct: float = 80.0
    explicit: bool = False


def _budget_from_mapping(spec: Any) -> ProviderBudget | None:
    """Parse one JSON budget entry; ``None`` if malformed (never raises)."""
    if not isinstance(spec, dict):
        return None
    try:
        rps = float(spec.get("rps", RPC_PROVIDER_RPS))
        max_concurrency = int(spec.get("max_concurrency", RPC_PROVIDER_MAX_CONCURRENCY))
        daily_budget = int(spec.get("daily_budget", 0))
        preemptive_pct = float(spec.get("preemptive_pct", RPC_BUDGET_PREEMPTIVE_PCT))
    except (TypeError, ValueError):
        return None
    return ProviderBudget(
        rps=max(0.0, rps),
        max_concurrency=max(0, max_concurrency),
        daily_budget=max(0, daily_budget),
        preemptive_pct=preemptive_pct,
        explicit=True,
    )


def parse_provider_budgets(raw: str) -> dict[str, ProviderBudget]:
    """Parse an operator budget map (JSON object keyed by identity).

    Malformed input degrades to an empty map — a bad env var must never
    stop the indexer from starting.
    """
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, ProviderBudget] = {}
    for identity, spec in data.items():
        budget = _budget_from_mapping(spec)
        if budget is not None and identity:
            out[str(identity)] = budget
    return out


RPC_PROVIDER_BUDGETS: dict[str, ProviderBudget] = parse_provider_budgets(os.environ.get("RPC_PROVIDER_BUDGETS", ""))
RPC_ENDPOINT_BUDGETS: dict[str, ProviderBudget] = parse_provider_budgets(os.environ.get("RPC_ENDPOINT_BUDGETS", ""))

#: Generic keyless default (explicit=False → the additive enforcement layer
#: is a no-op for it; existing provider caps already pace this profile).
DEFAULT_PROVIDER_BUDGET: ProviderBudget = ProviderBudget(
    rps=RPC_PROVIDER_RPS,
    max_concurrency=RPC_PROVIDER_MAX_CONCURRENCY,
    daily_budget=RPC_PROVIDER_DAILY_BUDGET,
    preemptive_pct=RPC_BUDGET_PREEMPTIVE_PCT,
    explicit=False,
)


def resolve_provider_budget(
    provider_id: str,
    endpoint: str | None = None,
    *,
    provider_budgets: dict[str, ProviderBudget] | None = None,
    endpoint_budgets: dict[str, ProviderBudget] | None = None,
    default: ProviderBudget | None = None,
) -> ProviderBudget:
    """Resolve the budget for an identity.

    Precedence: exact endpoint URL override → provider-account id override
    → generic default. Lookup maps/defaults are injectable for tests.
    """
    if endpoint_budgets is None:
        endpoint_budgets = RPC_ENDPOINT_BUDGETS
    if provider_budgets is None:
        provider_budgets = RPC_PROVIDER_BUDGETS
    if default is None:
        default = DEFAULT_PROVIDER_BUDGET
    if endpoint and endpoint in endpoint_budgets:
        return endpoint_budgets[endpoint]
    if provider_id and provider_id in provider_budgets:
        return provider_budgets[provider_id]
    return default


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
# KeyedProvider, then SolanaProvider (all free tiers, keys via env). The first successful
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

# Multi-provider fallback endpoints. Keyless public nodes are the zero-config
# default; keyed providers are OPT-IN and supplied from the environment or the
# private endpoint overlay — the public tree bakes in no provider URLs.
# Templates keep ${ENV_VAR} placeholders — never a real key in code; unset
# templates are dropped from the provider list at import.
SOLANA_PUBLIC_RPC: str = os.environ.get("SOLANA_PUBLIC_RPC", "https://api.mainnet-beta.solana.com")
SOLANA_PUBLIC_RPC_FALLBACK: str = os.environ.get("SOLANA_PUBLIC_RPC_FALLBACK", "https://solana-rpc.publicnode.com")
# Optional keyed provider RPC (any JSON-RPC provider). Empty = disabled.
SOLANA_PROVIDER_RPC_TEMPLATE: str = os.environ.get("SOLANA_PROVIDER_RPC_TEMPLATE", "")
# Optional provider-specific enhanced-history RPC (e.g. closed-account
# history). Empty = disabled; only tried when the standard path is empty.
SOLANA_ENHANCED_RPC_TEMPLATE: str = os.environ.get("SOLANA_ENHANCED_RPC_TEMPLATE", "")

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

# ── Cross-provider consensus verification (rpc/consensus.py) ───────────
# A sampled fraction of critical reads (eth_getLogs range results) is
# dispatched to N endpoints owned by DISTINCT provider accounts; the answer
# is accepted only when a quorum returns the same content hash. This is the
# client-side equivalent of managed-tier multi-upstream quorum and is the
# guard against a single malicious/buggy free RPC poisoning the index.
# Sampling bounds the cost: only RPC_CONSENSUS_SAMPLE_RATE of reads pay the
# extra requests. 0 disables sampling; >= 1 verifies every read.
RPC_CONSENSUS_ENABLED: bool = os.environ.get("RPC_CONSENSUS_ENABLED", "1") != "0"
RPC_CONSENSUS_MIN_AGREEMENT: int = int(os.environ.get("RPC_CONSENSUS_MIN_AGREEMENT", "2"))
RPC_CONSENSUS_SAMPLE_RATE: float = float(os.environ.get("RPC_CONSENSUS_SAMPLE_RATE", "0.05"))

# ── EVM WebSocket head pool (rpc/head_pool.py) ─────────────────────────
# Push the chain tip instead of polling eth_blockNumber every cycle:
# subscribe to ``eth_subscribe(["newHeads"])`` over a single websocket to a
# healthy endpoint and wake the per-chain sync loop on each new head. The
# websocket URLs come ONLY from the private endpoint overlay (``ws`` list or
# ``ws://`` / ``wss://`` entries) — the public tree names no provider. With
# no WS endpoint configured the pool is a no-op and HTTP polling remains the
# sole tip source. A WS failure never stalls the indexer: the pool logs
# ``ws_head_fallback`` and polling continues as the heartbeat.
RPC_WS_HEAD_ENABLED: bool = os.environ.get("RPC_WS_HEAD_ENABLED", "1") != "0"
RPC_WS_HEAD_MAX_ENDPOINTS: int = int(os.environ.get("RPC_WS_HEAD_MAX_ENDPOINTS", "2"))
# Base/max for the jittered reconnect backoff (seconds).
RPC_WS_HEAD_RECONNECT_BASE_SECONDS: float = float(os.environ.get("RPC_WS_HEAD_RECONNECT_BASE_SECONDS", "1.0"))
RPC_WS_HEAD_RECONNECT_MAX_SECONDS: float = float(os.environ.get("RPC_WS_HEAD_RECONNECT_MAX_SECONDS", "30.0"))
RPC_CONSENSUS_MAX_ENDPOINTS: int = int(os.environ.get("RPC_CONSENSUS_MAX_ENDPOINTS", "3"))
# Intrinsic data-integrity validators (eRPC-inspired): before counting
# agreement, validate each provider's payload against the REQUEST (getLogs
# block range, monotonic (block, logIndex), non-empty when expected). Default
# OFF / observe-only — violations are logged + counted but DO NOT change which
# result is accepted. Set RPC_CONSENSUS_VALIDATORS=1 to reject violating
# answers from the quorum (fail-closed on malformed data).
RPC_CONSENSUS_VALIDATORS: bool = os.environ.get("RPC_CONSENSUS_VALIDATORS", "0") == "1"

# Adaptive consensus budgeting: spend verification where risk is. The
# effective sample rate moves between MIN and MAX based on an EWMA of recent
# mismatch rate plus live health signals (endpoint error rate, stale-tip
# demotions, unvetted endpoints). When off, the fixed RPC_CONSENSUS_SAMPLE_RATE
# (clamped into [MIN, MAX]) is used. RPC_CONSENSUS_ENABLED still gates all
# verification.
RPC_CONSENSUS_ADAPTIVE: bool = os.environ.get("RPC_CONSENSUS_ADAPTIVE", "1") == "1"
RPC_CONSENSUS_SAMPLE_MIN: float = float(os.environ.get("RPC_CONSENSUS_SAMPLE_MIN", "0.01"))
RPC_CONSENSUS_SAMPLE_MAX: float = float(os.environ.get("RPC_CONSENSUS_SAMPLE_MAX", "0.25"))
# An endpoint counts as "unvetted" until this many calls have been recorded;
# adaptive risk rises while the pool is fresh.
RPC_CONSENSUS_UNVETTED_CALLS: int = int(os.environ.get("RPC_CONSENSUS_UNVETTED_CALLS", "3"))
