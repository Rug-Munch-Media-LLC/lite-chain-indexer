"""
Configuration constants for LiteChainIndexer.

All tunable parameters live here — RPC timeouts, batch sizes,
disk caps, event topics, and factory addresses.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# Load the shared env file (same file the daemon's systemd unit sources
# via EnvironmentFile) BEFORE any os.environ reads below, so every entry
# point — daemon, health module, integrity checks, cron, manual runs —
# resolves the same ${ALCHEMY_KEY} / ${INFURA_KEY} / ${HELIUS_KEY} RPC
# placeholders. Idempotent and non-destructive (existing env wins).
from lite_indexer.env import load_env_file

load_env_file()

# ── Chain-Indexer Directories ─────────────────────────────────────────
DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/data/chain-indexer"))
STATE_DIR = DATA_DIR / "state"

# Hard disk cap: 10 GB
DISK_CAP_BYTES = 10 * 1024**3

# Max blocks per batch (eth_getLogs over too-large ranges can timeout)
BATCH_SIZE = 1000

# Max events per batch (safety limit)
MAX_EVENTS_PER_BATCH = 100_000

# Blocks behind chain tip when --start-from-head is used
HEAD_BUFFER = 100

# RPC call timeout for eth_getLogs (longer since it can return large data)
RPC_TIMEOUT = 30

# Max backoff for recently-failed endpoints (seconds)
BACKOFF_CAP = 8

# Max wait for rate-limited endpoints (seconds)
RATE_LIMIT_WAIT_CAP = 16

# Default timeout for RPC calls
RPC_CALL_TIMEOUT = 15.0

# Timeout for block timestamp fetch calls
TIMESTAMP_FETCH_TIMEOUT = 15

# Semaphore limit for parallel RPC calls
GET_LOGS_SEMAPHORE_LIMIT = 10

# Minimum free disk space (bytes) — 1GB
MIN_FREE_DISK_BYTES = 1_000_000_000

# ClickHouse target tables
CH_TABLE = "wallet_memory.evm_transfers"
CH_DEX_TABLE = "wallet_memory.dex_events"
CH_DEPLOYER_TABLE = "wallet_memory.deployer_events"

# ── Internal-Transfer Trace Collection ────────────────────────────────
# The indexer normally only sees ERC-20 Transfer logs. Internal ETH flows
# (WETH unwraps, bridge deposits, fee flows, SELFDESTRUCT payouts) are
# invisible to eth_getLogs and live in wallet_memory.internal_transfers,
# collected from free trace_* / debug_traceBlockByNumber RPCs. Default
# ON — the collector is stable, bounded per-cycle (TRACE_MAX_BLOCKS_PER_CYCLE),
# and rate-limit-aware (TRACE_CONCURRENCY). Disable with TRACE_ENABLED=0 or
# --no-traces CLI flag.
TRACE_ENABLED: bool = os.environ.get("TRACE_ENABLED", "1") == "1"

# Trace calls can be slow on busy blocks (a full block trace returns a
# nested call tree) — use a longer timeout than the default RPC call.
TRACE_CALL_TIMEOUT: float = float(os.environ.get("TRACE_CALL_TIMEOUT", "60.0"))

# Max blocks swept per sync cycle when traces are enabled (bounds RPC
# load on free tiers — trace endpoints are rate-limited far below the
# getLogs tier).
TRACE_MAX_BLOCKS_PER_CYCLE: int = int(os.environ.get("TRACE_MAX_BLOCKS_PER_CYCLE", "100"))

# Concurrent trace calls per chain (trace endpoints are rate-limited).
TRACE_CONCURRENCY: int = int(os.environ.get("TRACE_CONCURRENCY", "2"))

# ClickHouse table for internal-transfer rows.
CH_TRACE_TABLE: str = "wallet_memory.internal_transfers"

# ── Cross-Chain Table References ───────────────────────────────────────
# Read-side table references. The main transfer table is CH_TABLE above;
# these aliases keep readers decoupled from the writer's naming so either
# can evolve independently.
CH_DATABASE: str = os.environ.get("CH_DATABASE") or os.environ.get("CLICKHOUSE_DATABASE") or "wallet_memory"
CH_EVM_TRANSFERS_TABLE: str = os.environ.get("CH_EVM_TRANSFERS_TABLE") or CH_TABLE
CH_INTERNAL_TRANSFERS_TABLE: str = os.environ.get("CH_INTERNAL_TRANSFERS_TABLE") or CH_TRACE_TABLE
CH_SOLANA_TABLE: str = os.environ.get("CH_SOLANA_TABLE") or "wallet_memory.solana_spl_balance_changes"
CH_LABELS_TABLE: str = os.environ.get("CH_LABELS_TABLE") or "wallet_memory.wallet_labels"
CH_SCAM_TABLE: str = os.environ.get("CH_SCAM_TABLE") or "wallet_memory.scam_addresses"
CH_BRIDGE_MAPPINGS_TABLE: str = os.environ.get("CH_BRIDGE_MAPPINGS_TABLE") or "wallet_memory.bridge_mappings"

# ── Event Topics ──────────────────────────────────────────────────────

# keccak256("Transfer(address,address,uint256)")
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# keccak256("Swap(address,uint256,uint256,uint256,uint256,address)")
SWAP_V2_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

# keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)")
SWAP_V3_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

# keccak256("PairCreated(address,address,address,uint256)")
# Verified on-chain: 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# keccak256("PoolCreated(address,address,uint24,int24,address)")
POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

# ── eth_call Selectors ────────────────────────────────────────────────

TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"

# Max concurrent eth_call fallback requests for pool token resolution
MAX_CONCURRENT_POOL_RESOLVE = 5

# ── Endpoint Health Tracking ──────────────────────────────────────────

ENDPOINT_COOLDOWN = 300
MAX_FAILURES = 3

# How often (in seconds) to re-verify an endpoint after a successful check
ENDPOINT_VERIFY_INTERVAL = 3600  # 1 hour

# How long to wait for an endpoint verification response
ENDPOINT_VERIFY_TIMEOUT = 10.0

# Block range to use for endpoint health verification (small to avoid range errors)
VERIFY_BLOCK_DELTA = 100  # Check 100 blocks back from tip

# ── Average Block Times ─────────────────────────────────────────────
# Per-chain average block time in seconds, used when estimating block
# timestamps when RPC timestamp lookup fails. Hardcoding a single 12s
# value badly skews estimates for fast chains (Base 2s, Arbitrum 0.25s).
DEFAULT_BLOCK_TIME = 12.0

CHAIN_BLOCK_TIMES: dict[str, float] = {
    "ethereum": 12.0,
    "base": 2.0,
    "bsc": 3.0,
    "polygon": 2.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "avalanche": 2.0,
    "fantom": 1.0,
    "linea": 2.0,
    "scroll": 3.0,
    "zksync": 2.0,
    "mantle": 2.0,
    "gnosis": 5.0,
    "celo": 5.0,
    "solana": 0.4,
    "tron": 3.0,
}

# ── Known DEX Factory Addresses ───────────────────────────────────────
# V2 factories emit PairCreated(address,address,address,uint256) (Uniswap
# V2 topic). V3 factories emit PoolCreated(address,address,uint24,int24,address).
# Addresses verified against official docs / block explorers 2026-08-01.

FACTORY_ADDRESSES: dict[str, list[str]] = {
    "ethereum": ["0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"],  # Uniswap V2
    "base": ["0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6"],
    "bsc": ["0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"],  # PancakeSwap
    "polygon": ["0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32"],  # QuickSwap
    "arbitrum": ["0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9"],
    "avalanche": [
        "0x9e5A52f57b3038F1B8EeE45F28b3C1967e22799C",  # Uniswap V2
        "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",  # Trader Joe V1 (LFJ)
    ],
    "linea": ["0x02a84c1b3BBD7401a5f7fa98a384EBC70bB5749E"],  # PancakeSwap V2
    "zksync": ["0xd03D8D566183F0086d8D09A84E1e30b58Dd5619d"],  # PancakeSwap V2
    "mantle": ["0x5bEf015CA9424A7C07B68490616a4C1F094BEdEc"],  # Merchant Moe
    "gnosis": ["0xc35DADB65012eC5796536bD9864eD8773aBc74C4"],  # SushiSwap V2
    "celo": [
        "0x62d5b84bE28a183aBB507E125B384122D2C25fAE",  # Ubeswap V2
        "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",  # SushiSwap V2
    ],
    "fantom": [
        "0x152eE697f2E276fA89E96742e9bB9aB1F2E61bE3",  # SpookySwap V2
        "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",  # SushiSwap V2
    ],
}

V3_FACTORY_ADDRESSES: dict[str, list[str]] = {
    "ethereum": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],  # Uniswap V3
    "base": ["0x33128a8fC17869897dcE68Ed026d694621f6FDfD"],
    "bsc": ["0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7"],  # PancakeSwap V3
    "arbitrum": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
    "optimism": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
    "polygon": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
    "avalanche": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],  # Uniswap V3
    "linea": ["0x31FAfd4889FA1269F7a13A66eE0fB458f27D72A9"],  # Uniswap V3
    "scroll": ["0x70C62C8b8e801124A4Aa81ce07b637A3e83cb919"],  # Uniswap V3 (Scroll Foundation)
    "zksync": ["0x8FdA5a7a8dCA67BBcDd10F02Fa0649A937215422"],  # Uniswap V3 (zkSync fork)
    "mantle": ["0xEaD128BDF9Cff441eF401Ec8D18a96b4A2d25252"],  # Uniswap V3
    "gnosis": ["0xe32F7dD7e3f098D518ff19A22d5f028e076489B1"],  # Uniswap V3
    "celo": ["0xAfE208a311B21f13EF87E33A90049fC17A7acDEc"],  # Uniswap V3
}


# Alias for backward-compatibility in indexer imports
CHAIN_INDEXER_DIR = DATA_DIR

# ── Chunk-Based Indexing ───────────────────────────────────────────────
# Instead of per-block polling, block ranges are split into configurable
# chunks and fetched as one eth_getLogs call per chunk, processed in
# parallel. Defaults: 50 blocks for EVM, 500 slots for Solana.
CHUNK_SIZE_EVM: int = 50
CHUNK_SIZE_SOLANA: int = 500

# Max concurrent chunk fetches per range (parallelism cap).
CHUNK_PARALLELISM: int = 4

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

# ── ClickHouse Retention / Column Pruning ─────────────────────────────
# Keep full-precision rows for N days, then degrade non-summary columns
# to their type defaults via column-level TTL and (optionally) delete
# expired rows. Cuts storage 30-50% while keeping summary queries fast.
CH_TTL_ENABLED: bool = os.environ.get("CH_TTL_ENABLED", "1") != "0"

# ── Wallet Access Path Optimizations ──────────────────────────────────
# Each competitive feature below is independently usable and gated by
# its own env flag (default: on). All DDL is idempotent and non-fatal —
# a missing table or missing privileges degrades to a debug log, exactly
# like the retention policies.

# 1. Address-first access path (electrs-inspired): add projections on
#    evm_transfers ordered by (chain, address, block_number, log_index)
#    so "all transfers for wallet X" is served by an address-keyed
#    projection instead of a full table scan. ClickHouse projections
#    cannot UNION ALL, so the wallet history is split into two
#    projections: one keyed on from_address (out), one on to_address (in).
WALLET_PROJECTION_ENABLED: bool = os.environ.get("WALLET_PROJECTION_ENABLED", "1") != "0"

# 2. Token metadata reference table (cryo-inspired): one row per token in
#    wallet_memory.token_meta_ref instead of token_symbol/token_name on
#    every transfer row (~60 bytes x 278M rows). Always enabled and
#    non-configurable — the reference table approach is proven correct and
#    enrichment is never written to transfer rows anymore. Transfer rows
#    store empty symbol/name; the reference table is upserted once per
#    unique token per batch.
TOKEN_META_ENABLED: bool = True

# 3. Per-token incremental balance cache (electrs lazy cache): maintain
#    wallet_memory.holder_balances incrementally per transfer so holder
#    queries never aggregate the full evm_transfers table.
BALANCE_CACHE_ENABLED: bool = os.environ.get("BALANCE_CACHE_ENABLED", "1") != "0"

# 4. Auto-maintained holder balances: a SummingMergeTree materialized
#    view fed by evm_transfers inserts. The answer is pre-computed and
#    maintained by ClickHouse itself (no indexer-side read-modify-write).
HOLDER_BALANCES_MV_ENABLED: bool = os.environ.get("HOLDER_BALANCES_MV_ENABLED", "1") != "0"

# 5. DEX skip-to-head (env-gated, default OFF): when the sync window is
#    deeper than DEX_SKIP_TO_HEAD_HOURS behind the chain head, skip the
#    dex getLogs fetch for that cycle — old-block dex rows are useless to
#    the scanners and the freed RPC budget accelerates the transfer
#    catch-up. Dex resumes automatically once transfers reach fresh
#    blocks. NOTE: skipped ranges stay a hole in dex_events until a
#    backfill runs — enable only when draining a deep backlog fast
#    matters more than historical completeness.
DEX_SKIP_TO_HEAD: bool = os.environ.get("DEX_SKIP_TO_HEAD", "0") != "0"
DEX_SKIP_TO_HEAD_HOURS: int = int(os.environ.get("DEX_SKIP_TO_HEAD_HOURS", "24") or "24")

# Wallet-access table / projection names.
CH_TOKEN_META_TABLE: str = "wallet_memory.token_meta_ref"
CH_HOLDER_BALANCES_TABLE: str = "wallet_memory.holder_balances"
CH_HOLDER_BALANCES_MV: str = "wallet_memory.holder_balances_mv"
WALLET_OUT_PROJECTION: str = "wallet_out_proj"
WALLET_IN_PROJECTION: str = "wallet_in_proj"

# Retention (days) per table before column-level degradation.
RETENTION_DAYS: dict[str, int] = {
    "evm_transfers": 30,
    "dex_events": 90,
    "deployer_events": 365,
    "internal_transfers": 90,
}

# Columns to KEEP (summary) for old rows — everything else degrades.
# evm_transfers summary: from, to, token, value, block_ts (plus row keys)
# dex_events summary: swap identity + pair + tokens (details degrade)
EVT_SUMMARY_COLUMNS: tuple[str, ...] = (
    "chain",
    "block_number",
    "tx_hash",
    "from_address",
    "to_address",
    "token_address",
    "value",
    "block_timestamp",
    "log_index",
)
DEX_SUMMARY_COLUMNS: tuple[str, ...] = (
    "type",
    "chain",
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "contract_address",
    "dex",
    "version",
    "token0",
    "token1",
)

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

# ── Multi-Chain Parallelism ───────────────────────────────────────────
# Per-chain CPU affinity (Linux only, best-effort). Used when running a
# single chain per process; the multi-chain daemon uses priorities.
CHAIN_AFFINITY_ENABLED: bool = os.environ.get("CHAIN_AFFINITY_ENABLED", "0") == "1"

# Max number of chains that sync CONCURRENTLY in daemon mode. 14 chains
# running at once is what caused the 08/03 OOM kills (56 cgroup kills,
# 63 service restarts): each chain holds its batch + event lists in
# memory while fetching. Capping concurrency bounds peak RSS to roughly
# `MAX_CONCURRENT_CHAINS × per-chain budget`. High-priority chains get
# slots first (task creation order); lower-priority chains rotate in as
# slots free. Set MAX_CONCURRENT_CHAINS=14 to restore old behavior.
MAX_CONCURRENT_CHAINS: int = int(os.environ.get("MAX_CONCURRENT_CHAINS", "6"))

# ── Chain-Level Health / Auto-Disable (daemon) ────────────────────────
# If a chain's state has not advanced for CHAIN_STALL_DISABLE_SECONDS
# (default 2h) the daemon marks it disabled and stops spending RPC budget
# on it, while the other chains keep indexing. A disabled chain is
# re-probed at a slow interval and re-enables as soon as its state moves
# again (e.g. the RPC pool for that chain recovers).
CHAIN_AUTO_DISABLE_ENABLED: bool = os.environ.get("CHAIN_AUTO_DISABLE_ENABLED", "1") == "1"
CHAIN_STALL_DISABLE_SECONDS: int = int(os.environ.get("CHAIN_STALL_DISABLE_SECONDS", "7200"))
# Re-probe interval (seconds) for a disabled chain.
CHAIN_DISABLED_RECHECK_SECONDS: int = int(os.environ.get("CHAIN_DISABLED_RECHECK_SECONDS", "900"))

# Run gc.collect() after every N sync cycles per chain. Python's cyclic
# GC does not collect aggressively under cgroup memory pressure; event
# dicts, bloom filters, and httpx response objects form reference cycles
# that a manual collect reclaims between cycles. Enforced inside
# LiteIndexer.sync_loop (covers daemon AND single-chain mode).
GC_EVERY_N_CYCLES: int = int(os.environ.get("GC_EVERY_N_CYCLES", "5"))

# ── Memory Watchdog ──────────────────────────────────────────────────────
# When the indexer process RSS exceeds MEMORY_HIGH_MB the daemon pauses
# NEW chain ingestion (no new sync cycles) until RSS drops back under the
# threshold. This is an in-process backstop on top of the systemd
# MemoryHigh/MemoryMax cgroup limits: it avoids the OOM-kill churn that
# 14 parallel chains caused on 2026-08-03 by backing off *before* the
# cgroup reaper fires. Default 6144 = 6 GiB, matching the systemd
# MemoryHigh drop-in (6442450944 bytes). Raised 2026-08-14 from 3072
# after the daemon's working set (16 chains × pair caches + httpx pools +
# event batches) grew past the 3G ceiling, which froze the process in
# kernel cgroup throttling (mem_cgroup_handle_over_high, D-state) instead
# of letting the gate pause ingestion gracefully. RSS is read from
# /proc/self/statm (no psutil dependency).
MEMORY_HIGH_MB: int = int(os.environ.get("MEMORY_HIGH_MB", "6144"))

# How often (seconds) the memory watchdog may re-read RSS. Reading
# /proc every cycle is cheap, but under heavy GC churn the value flaps;
# caching it bounds the backoff logic.
MEMORY_CHECK_INTERVAL: int = int(os.environ.get("MEMORY_CHECK_INTERVAL", "15"))

# How long (seconds) a chain loop sleeps when the memory gate is closed
# (RSS over MEMORY_HIGH_MB) before re-checking.
MEMORY_PAUSE_SLEEP: int = int(os.environ.get("MEMORY_PAUSE_SLEEP", "30"))

# ── ClickHouse Health Pause ─────────────────────────────────────────────
# Graceful degradation when ClickHouse is unreachable: the sync loop
# probes CH before fetching events from RPC and, when CH is down, aborts
# the cycle with status "ch_unavailable" WITHOUT advancing state and
# WITHOUT burning free-tier RPC budget on fetches it cannot persist.
# The daemon treats "ch_unavailable" as a pause (no state movement, no
# error spam) rather than a crash.
CH_HEALTH_CHECK_ENABLED: bool = os.environ.get("CH_HEALTH_CHECK_ENABLED", "1") == "1"

# How often (seconds) the CH health probe result is cached before the
# sync loop re-probes (probe = SELECT 1 via the existing CH client).
CH_HEALTH_INTERVAL: int = int(os.environ.get("CH_HEALTH_INTERVAL", "30"))

# ── State Backward Protection ───────────────────────────────────────────
# Guard against state-file regressions (observed 08/03 + 08/04: the
# ethereum state file was reset to ~93K / block 100 while the ClickHouse
# max was 25.6M, triggering an endless genesis backfill). When enabled,
# the indexer keeps a monotonic high-water mark per chain and, on load,
# detects when the state file's last_block regressed by more than
# STATE_REGRESSION_TOLERANCE_BLOCKS below the high-water mark. It then
# recovers by querying ClickHouse for the actual max block and resuming
# from there instead of re-indexing from genesis.
STATE_BACKWARD_PROTECTION: bool = os.environ.get("STATE_BACKWARD_PROTECTION", "1") == "1"

# A state regression smaller than this is treated as legitimate reorg
# rewind (reorg rollbacks rewind state to fork_block-1, typically a few
# blocks) and is NOT recovered. Only a larger gap is corruption.
STATE_REGRESSION_TOLERANCE_BLOCKS: int = int(os.environ.get("STATE_REGRESSION_TOLERANCE_BLOCKS", "100"))

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

# ── Periodic Data Reconciliation ────────────────────────────────────────
# Every DATA_RECONCILIATION_EVERY_N_CYCLES sync cycles, compare the
# ClickHouse row count for the just-synced range against the events
# fetched from RPC. A discrepancy above
# DATA_RECONCILIATION_MAX_DISCREPANCY_PCT percent triggers a warning and
# rewinds the state file so the next cycle re-fetches (backfills) the
# range — catching partial writes / missed chunks before they compound.
DATA_RECONCILIATION_ENABLED: bool = os.environ.get("DATA_RECONCILIATION_ENABLED", "1") == "1"
DATA_RECONCILIATION_EVERY_N_CYCLES: int = int(os.environ.get("DATA_RECONCILIATION_EVERY_N_CYCLES", "50"))
DATA_RECONCILIATION_MAX_DISCREPANCY_PCT: float = float(os.environ.get("DATA_RECONCILIATION_MAX_DISCREPANCY_PCT", "5.0"))

CHAIN_CPU_AFFINITY: dict[str, list[int]] = {
    "solana": [0, 1],
    "tron": [8, 9],
    # EVM chains (default affinity 2-7)
    "ethereum": [2, 3, 4, 5],
    "base": [2, 3, 4, 5],
    "bsc": [2, 3, 4, 5],
    "polygon": [2, 3, 4, 5],
    "arbitrum": [2, 3, 4, 5],
    "optimism": [2, 3, 4, 5],
}

# Per-chain memory budget (MB) — Solana's large getBlock responses need
# the most headroom. Used for logging and optional resource capping.
CHAIN_MEMORY_BUDGET_MB: dict[str, int] = {
    "solana": 2048,
    "tron": 1024,
    "ethereum": 768,
    "base": 512,
    "bsc": 512,
    "polygon": 512,
    "arbitrum": 512,
    "optimism": 512,
    "avalanche": 512,
    "fantom": 384,
    "linea": 384,
    "scroll": 384,
    "zksync": 384,
    "mantle": 384,
    "gnosis": 384,
    "celo": 384,
    "robinhood": 384,
}

# Priority (1-10): high-value chains poll more frequently. Ethereum and
# Solana are the highest-value chains.
CHAIN_PRIORITY: dict[str, int] = {
    "ethereum": 10,
    "solana": 10,
    "base": 8,
    "arbitrum": 8,
    "polygon": 8,
    "bsc": 7,
    "optimism": 7,
    "tron": 7,
    "avalanche": 6,
    "linea": 6,
    "scroll": 6,
    "zksync": 6,
    "mantle": 6,
    "fantom": 5,
    "gnosis": 5,
    "celo": 5,
    "robinhood": 5,  # New chain — low priority until demand proven
}

# ── Parquet Cold Storage Tier ─────────────────────────────────────────
# Daily rollup of old ClickHouse rows into Parquet, uploaded to the B2
# permanent tier, then deleted from ClickHouse to reclaim disk.
# Optional dependency: pyarrow.
PARQUET_DIR: Path = DATA_DIR / "parquet"
# Remote root for the parquet archive. Canonical prefix (matches the
# live hourly export script /root/scripts/lite-indexer-parquet-daily.sh
# and the DuckDB read path in lite_indexer.storage.duckdb_query).
PARQUET_REMOTE: str = os.environ.get("PARQUET_REMOTE", "b2-crypt:chain-data/parquet")
PARQUET_RCLONE_BIN: str = os.environ.get("PARQUET_RCLONE_BIN", "rclone")
# Min age (days) of rows eligible for cold storage (keep hot window).
# Used for the --days backfill mode; the daily job exports an explicit
# date (--date) instead.
PARQUET_COLD_DAYS: int = int(os.environ.get("PARQUET_COLD_DAYS", "7"))
# State dir records which (chain, date) files were already exported so
# the daily job is idempotent even when ClickHouse is unavailable.
PARQUET_STATE_DIR: Path = STATE_DIR / "parquet"
# ClickHouse audit table recording every (chain, date) export attempt
# and its status (pending / exporting / uploaded / deleted / failed).
CH_PARQUET_STATE_TABLE: str = "wallet_memory.parquet_export_state"

# ── Snapshot Bootstrap (public parquet replay) ─────────────────────────
# ``lite-indexer bootstrap`` downloads public parquet snapshots from B2
# and imports them into ClickHouse in minutes instead of re-indexing a
# chain from genesis over free RPCs. BOOTSTRAP_B2_BUCKET is the rclone
# remote root that also holds the live cold tier; BOOTSTRAP_PARQUET_PATH
# is the prefix inside it where snapshot folders ({date}/{chain}/) live.
BOOTSTRAP_B2_BUCKET: str = os.environ.get("BOOTSTRAP_B2_BUCKET", "b2-crypt:chain-data")
BOOTSTRAP_PARQUET_PATH: str = os.environ.get("BOOTSTRAP_PARQUET_PATH", "snapshots")
# Local staging dir for downloaded snapshots before the ClickHouse import.
BOOTSTRAP_LOCAL_DIR: Path = Path(os.environ.get("BOOTSTRAP_LOCAL_DIR", str(DATA_DIR / "bootstrap")))
# Optional mount root for snapshot parquet files (empty = use local dir).
BOOTSTRAP_CH_FILE_ROOT: str = os.environ.get("BOOTSTRAP_CH_FILE_ROOT", "")
# rclone timeout (seconds) for snapshot downloads/uploads.
BOOTSTRAP_RCLONE_TIMEOUT: int = int(os.environ.get("BOOTSTRAP_RCLONE_TIMEOUT", "1200"))

# ── Transparent Historical Access (THA) ────────────────────────────────
# Unified hot (ClickHouse) + cold (B2 parquet) reads through one access
# layer (lite_indexer.storage.historical.HistoricalAccess). When enabled,
# the API's query() consults the access layer so every read spans the
# whole archive transparently: hot rows (< HISTORICAL_HOT_TTL_DAYS) come
# from ClickHouse for sub-second reads, cold rows back to genesis come
# from B2 parquet via DuckDB. Disabling keeps the legacy hot-only path.
HISTORICAL_ACCESS_ENABLED: bool = os.environ.get("HISTORICAL_ACCESS_ENABLED", "1") != "0"

# How many days of data are considered "hot" (kept at full fidelity in
# ClickHouse). Auto-detected from the table TTL expression when possible;
# this value is the fallback and the floor for detection.
HISTORICAL_HOT_TTL_DAYS: int = int(os.environ.get("HISTORICAL_HOT_TTL_DAYS", "30"))

# Port for the rclone HTTP proxy that serves B2 parquet to ClickHouse's
# url() table function (scripts/serve-b2-http.sh). Also the port the
# *_historical views in migrations/006_historical_views.sql read from.
B2_HTTP_PROXY_PORT: int = int(os.environ.get("B2_HTTP_PROXY_PORT", "8090"))

# Timeout (seconds) for cold queries. Cold reads are slower than hot
# reads (DuckDB over parquet on B2), so they get their own budget. The
# timeout is enforced at the async boundary (the DuckDB thread is not
# cancellable, but the caller is never blocked past this budget).
COLD_QUERY_TIMEOUT: int = int(os.environ.get("COLD_QUERY_TIMEOUT", "60"))

# rclone remote root for the cold parquet tier. Defaults to the live
# daily-export location (PARQUET_REMOTE); the bootstrap bucket root is
# the same remote with a different prefix, so b2_bucket defaults here.
HISTORICAL_PARQUET_REMOTE: str = os.environ.get("HISTORICAL_PARQUET_REMOTE", PARQUET_REMOTE)

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

# ── NDJSON Write-Ahead Log ─────────────────────────────────────────────
# Redundant local replay buffer (events.jsonl per chain/date). The durable
# store is ClickHouse, so this is DISABLED by default — it costs ~15% CPU
# (zstd compression every 15 min, rglob disk scans per write, per-write
# disk-cap checks) plus a separate cron rotation. The code is retained
# behind this flag for rollback.
#
# Recovery path WITHOUT NDJSON: restart from state.to_block. On crash we
# lose at most one sync cycle (~30s of data), which the next cycle
# re-fetches from RPC. No replay buffer is required.
NDJSON_ENABLED: bool = os.environ.get("NDJSON_ENABLED", "0") == "1"

# ── Balance Reconciliation (TrueBlocks) ────────────────────────────────
# Compare computed balances from indexed evm_transfers deltas against
# on-chain state (eth_getBalance). Optional + non-blocking: only runs on
# a random sample of wallets (default 100 per chain) and caches results
# in wallet_memory.reconciliation_results.
RECONCILIATION_ENABLED: bool = os.environ.get("RECONCILIATION_ENABLED", "1") != "0"
RECONCILIATION_SAMPLE_SIZE: int = int(os.environ.get("RECONCILIATION_SAMPLE_SIZE", "100"))
CH_RECONCILIATION_TABLE: str = "wallet_memory.reconciliation_results"

# ── Reorg Staging (Envio/TrueBlocks) ──────────────────────────────────
# Track the last N blocks per chain as "unripe" using a ClickHouse
# checkpoint table (wallet_memory.block_checkpoints). On hash mismatch,
# roll back evm_transfers + dex_events above the fork point.
#
# Default ON for EVM chains. Solana/Tron never invoke the EVM reorg
# module (the staging lives under strategies/evm/), so protection is
# inherently off for non-EVM chains.
REORG_PROTECTION_ENABLED: bool = os.environ.get("REORG_PROTECTION_ENABLED", "1") != "0"
CH_BLOCK_CHECKPOINTS_TABLE: str = "wallet_memory.block_checkpoints"

# ── Lazy Indexing (per-chain event type gating) ─────────────────────────
# When enabled (default), every chain starts with transfer events only.
# DEX swap and deployer event indexing is enabled by default for top
# chains (see lite_indexer.chains.demand.TOP_CHAINS) and for long-tail
# chains only after the scanner (rmi-backend) signals demand via a file
# touch. Setting LAZY_INDEXING_ENABLED=0 restores the legacy behavior of
# indexing all event types equally (CLI flags are the authority).
LAZY_INDEXING_ENABLED: bool = os.environ.get("LAZY_INDEXING_ENABLED", "1") != "0"

# ── Plugin System ──────────────────────────────────────────────────────
# Directory where user-installed plugins live (LITE_INDEXER_PLUGIN_DIR,
# default DATA_DIR/plugins). A plugin is a folder containing a
# manifest.yaml + handler module; the loader scans this directory each
# sync cycle. The examples/plugins tree in the repo ships bundled
# plugins that `python -m lite_indexer.plugin.manager install` copies
# here. Set PLUGIN_ENABLED=0 to disable the plugin layer entirely.
PLUGIN_DIR: Path = Path(os.environ.get("LITE_INDEXER_PLUGIN_DIR", str(DATA_DIR / "plugins")))
PLUGIN_ENABLED: bool = os.environ.get("PLUGIN_ENABLED", "1") != "0"

# Directory where the scanner writes demand signal files:
#   {INDEXER_DEMAND_DIR}/{chain}/dex
#   {INDEXER_DEMAND_DIR}/{chain}/deployer
# The indexer polls these files each sync cycle and enables the requested
# event type for that chain. A file touch avoids cross-service imports.
INDEXER_DEMAND_DIR: Path = Path(os.environ.get("INDEXER_DEMAND_DIR", "/tmp/indexer-demand"))

# ── Solana Configuration ──────────────────────────────────────────────

SOLANA_SPL_TOKEN_PROGRAM: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

SOLANA_MAX_SIGNATURES_PER_TOKEN: int = 100
SOLANA_TX_CONCURRENCY: int = 10

SOLANA_CH_TABLE: str = "wallet_memory.solana_spl_balance_changes"

# ── Tron Configuration ────────────────────────────────────────────────

TRON_PAGE_SIZE: int = 200
TRON_MAX_BLOCK_RANGE: int = 2000

TRC20_CH_TABLE: str = "wallet_memory.evm_transfers"

# ── Whale Detection ───────────────────────────────────────────────────
# A DEX swap is a "whale" when:
#   1. USD notional of either leg >= WHALE_THRESHOLD_USD (requires a price
#      from the token_prices feed — see lite_indexer/pricing.py), OR
#   2. The raw token amount of either leg exceeds the per-chain
#      WHALE_AMOUNT_THRESHOLDS heuristic (used when no price is available;
#      base units, so the value assumes a typical 18-decimal EVM token —
#      Solana/Tron thresholds are scaled for 9/6 decimals).
# The `whale` column in wallet_memory.dex_events is UInt8 (0/1).
WHALE_THRESHOLD_USD: float = float(os.environ.get("WHALE_THRESHOLD_USD", "100000"))

# Per-chain raw-amount (base-unit) heuristic thresholds. These are a
# fallback for tokens without a USD price; USD-based detection (Fix 3) is
# the accurate path. Defaults approximate a $100K swap for a token priced
# near $1.00: 1e5 tokens x 10^decimals. Override wholesale with a JSON
# object in WHALE_AMOUNT_THRESHOLDS_JSON, e.g.
#   WHALE_AMOUNT_THRESHOLDS_JSON='{"ethereum": 1000000000000000000000000}'
WHALE_AMOUNT_THRESHOLDS_DEFAULT: dict[str, int] = {
    "ethereum": 10**23,
    "base": 10**23,
    "bsc": 10**23,
    "polygon": 10**23,
    "arbitrum": 10**23,
    "optimism": 10**23,
    "avalanche": 10**23,
    "linea": 10**23,
    "scroll": 10**23,
    "zksync": 10**23,
    "mantle": 10**23,
    "gnosis": 10**23,
    "celo": 10**23,
    "fantom": 10**23,
    "solana": 10**14,  # 9 decimals → 1e5 tokens
    "tron": 10**11,  # 6 decimals → 1e5 tokens
}

_whale_amounts_json = os.environ.get("WHALE_AMOUNT_THRESHOLDS_JSON", "")
WHALE_AMOUNT_THRESHOLDS: dict[str, int] = (
    {str(k): int(v) for k, v in json.loads(_whale_amounts_json).items()}
    if _whale_amounts_json
    else dict(WHALE_AMOUNT_THRESHOLDS_DEFAULT)
)

# Top-quantile per-pair raw amount marking (backfill only): a swap is a
# whale when its max leg amount is >= the 99.9th percentile for that pair
# over the scan window (i.e. the top 0.1%).
WHALE_TOP_QUANTILE: float = float(os.environ.get("WHALE_TOP_QUANTILE", "0.999"))

# ── USD Price Feed (CoinGecko / DefiLlama) ────────────────────────────
# Free tiers only — no API keys. Prices are cached in memory and mirrored
# to wallet_memory.token_prices (ReplacingMergeTree). On startup and every
# PRICING_REFRESH_INTERVAL seconds the indexer refreshes prices for the
# tokens recently seen in dex_events and uses them to (a) compute the
# whale flag at ingestion and (b) populate price_usd / volume_usd on DEX
# rows. Any provider failure degrades to the previous cached price —
# ingestion never blocks on the price feed.
PRICING_ENABLED: bool = os.environ.get("PRICING_ENABLED", "1") != "0"
PRICING_REFRESH_INTERVAL: int = int(os.environ.get("PRICING_REFRESH_INTERVAL", "900"))  # 15 min

CH_TOKEN_PRICES_TABLE: str = "wallet_memory.token_prices"

COINGECKO_BASE_URL: str = "https://api.coingecko.com/api/v3"
DEFILLAMA_BASE_URL: str = "https://coins.llama.fi"

# Min seconds between requests per provider (free-tier pacing).
COINGECKO_MIN_INTERVAL: float = float(os.environ.get("COINGECKO_MIN_INTERVAL", "12.0"))
DEFILLAMA_MIN_INTERVAL: float = float(os.environ.get("DEFILLAMA_MIN_INTERVAL", "2.0"))

# CoinGecko supports contract-address lookups per EVM/Tron platform. Map
# our internal chain names to CoinGecko asset platform ids.
COINGECKO_PLATFORMS: dict[str, str] = {
    "ethereum": "ethereum",
    "base": "base",
    "bsc": "binance-smart-chain",
    "polygon": "polygon-pos",
    "arbitrum": "arbitrum-one",
    "optimism": "optimistic-ethereum",
    "avalanche": "avalanche",
    "linea": "linea",
    "scroll": "scroll",
    "zksync": "zksync",
    "mantle": "mantle",
    "gnosis": "xdai",
    "celo": "celo",
    "fantom": "fantom",
    "tron": "tron",
}

# Max token addresses batched per provider request (free-tier URL length).
PRICING_BATCH_SIZE: int = int(os.environ.get("PRICING_BATCH_SIZE", "50"))

# Cap on distinct tokens fetched per refresh cycle (bounds API load on
# the free tiers; the highest-traffic tokens are sampled first).
PRICING_MAX_TRACKED_TOKENS: int = int(os.environ.get("PRICING_MAX_TRACKED_TOKENS", "2500"))

# How far back to scan dex_events for "recently seen" tokens.
PRICING_TOKEN_WINDOW_DAYS: int = int(os.environ.get("PRICING_TOKEN_WINDOW_DAYS", "7"))

# Default decimals when the price feed has no decimals info: EVM ERC-20s
# default to 18, Solana SPL to 9, Tron TRC-20 to 6. Overridable per token
# via KNOWN_DECIMALS in pricing.py (stablecoins are pre-seeded).
DEFAULT_DECIMALS_BY_CHAIN: dict[str, int] = {
    "ethereum": 18,
    "base": 18,
    "bsc": 18,
    "polygon": 18,
    "arbitrum": 18,
    "optimism": 18,
    "avalanche": 18,
    "linea": 18,
    "scroll": 18,
    "zksync": 18,
    "mantle": 18,
    "gnosis": 18,
    "celo": 18,
    "fantom": 18,
    "solana": 9,
    "tron": 6,
}

# ── Vector Ingestion (LanceDB) ──────────────────────────────────────────
# Optional background pipeline that turns freshly indexed ClickHouse rows
# into semantic embeddings (see lite_indexer/vectors/ingestion.py): it reads
# the last synced block range from ClickHouse, generates text representations
# for wallets and contracts, embeds them via Qwen3-Embedding-4B, and
# stores them in LanceDB collections. ON by default — the pipeline is stable
# and best-effort: a failure is logged and never blocks the core sync loop.
VECTOR_INGESTION_ENABLED: bool = os.environ.get("VECTOR_INGESTION_ENABLED", "1") == "1"

# Ingestion cadence: run the vector pipeline once every N blocks per chain
# (checked at the end of each sync cycle; the first cycle always ingests).
VECTOR_INGESTION_INTERVAL_BLOCKS: int = int(os.environ.get("VECTOR_INGESTION_INTERVAL_BLOCKS", "100"))

# Max wallets/contracts read + embedded per ingestion run. Bounds the embedding
# request size and memory usage of a single ingestion pass.
VECTOR_INGESTION_BATCH_SIZE: int = int(os.environ.get("VECTOR_INGESTION_BATCH_SIZE", "50"))

# ── NATS JetStream Real-Time Streaming ───────────────────────────────
# After ClickHouse writes succeed, the indexer fans events out to NATS
# for real-time consumers (scanner alerts, dashboards, bots). NATS is a
# fan-out layer, never the durable store — downtime must never block
# indexing, so publishing is best-effort and protected by a circuit
# breaker: after NATS_CIRCUIT_FAILURE_THRESHOLD consecutive failures the
# circuit opens and publishes are dropped for the cooldown period.
NATS_ENABLED: bool = os.environ.get("NATS_ENABLED", "1") == "1"

# NATS server URL (compose.nats.yml runs a JetStream-enabled server).
NATS_URL: str = os.environ.get("NATS_URL", "nats://localhost:4222")

# Circuit breaker: consecutive failures before the circuit opens.
NATS_CIRCUIT_FAILURE_THRESHOLD: int = int(os.environ.get("NATS_CIRCUIT_FAILURE_THRESHOLD", "5"))

# Circuit breaker: how long the circuit stays open (seconds) before a
# half-open probe is attempted.
NATS_CIRCUIT_COOLDOWN_SECONDS: int = int(os.environ.get("NATS_CIRCUIT_COOLDOWN_SECONDS", "30"))

# When true, events are published through JetStream (a stream matching
# `chain.>` / `alerts.>` is auto-provisioned on first connect). When the
# server has JetStream disabled the publisher degrades to core NATS
# publish automatically.
NATS_JETSTREAM_ENABLED: bool = os.environ.get("NATS_JETSTREAM_ENABLED", "1") != "0"

# JetStream stream name auto-provisioned to capture indexed events.
NATS_STREAM_NAME: str = os.environ.get("NATS_STREAM_NAME", "CHAIN_EVENTS")

# JetStream stream config. Defaults mirror the deployed
# lite-chain-indexer-nats server (compose.nats.yml + compose/nats-server.conf)
# and the live CHAIN_EVENTS stream: FILE storage on the persisted
# nats-data volume, limits retention, 3-day / 5GB cap, discard old.
# The publisher reconciles an existing stream whose config drifts (NATS
# err 10058) via update_stream — non-destructive, preserves messages +
# consumers. NATS err 10058 means "stream name already in use with a
# different configuration"; the publisher's declaration must match the
# server, otherwise every connect logs nats_stream_ensure_failed.
NATS_STREAM_STORAGE: str = os.environ.get("NATS_STREAM_STORAGE", "file")
NATS_STREAM_RETENTION: str = os.environ.get("NATS_STREAM_RETENTION", "limits")
NATS_STREAM_MAX_AGE_SECONDS: int = int(os.environ.get("NATS_STREAM_MAX_AGE_SECONDS", str(3 * 24 * 60 * 60)))
NATS_STREAM_MAX_BYTES: int = int(os.environ.get("NATS_STREAM_MAX_BYTES", str(5 * 1024**3)))
NATS_STREAM_DISCARD: str = os.environ.get("NATS_STREAM_DISCARD", "old")

# ── Webhook Delivery (push platform) ────────────────────────────────────
# POST every NATS event to registered webhooks (HMAC-SHA256 signed,
# bounded retries). Requires NATS_ENABLED=1 — the delivery service is a
# NATS subscriber. Default ON.
WEBHOOK_ENABLED: bool = os.environ.get("WEBHOOK_ENABLED", "1") != "0"

# SQLite persistence for subscriptions + delivery log.
WEBHOOK_DB_PATH: str = os.environ.get("WEBHOOK_DB_PATH", str(DATA_DIR / "webhooks.db"))

# Retry attempts after the initial POST (backoff 1s, 4s, 16s).
WEBHOOK_MAX_RETRIES: int = int(os.environ.get("WEBHOOK_MAX_RETRIES", "3"))

# Per-request HTTP timeout and total delivery budget (seconds).
WEBHOOK_TIMEOUT_SECONDS: float = float(os.environ.get("WEBHOOK_TIMEOUT_SECONDS", "10.0"))
WEBHOOK_TOTAL_TIMEOUT_SECONDS: float = float(os.environ.get("WEBHOOK_TOTAL_TIMEOUT_SECONDS", "30.0"))

# Backoff sequence between retries (seconds), matching .env.example.
WEBHOOK_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 4.0, 16.0)

# Per-IP cap on webhook registrations (API layer).
WEBHOOK_MAX_PER_IP: int = int(os.environ.get("WEBHOOK_MAX_PER_IP", "10"))
