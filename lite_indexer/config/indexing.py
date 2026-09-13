"""Indexing configuration for LiteChainIndexer."""

from __future__ import annotations

import json
import os
from pathlib import Path

# Load the shared env file (same file the daemon's systemd unit sources
# via EnvironmentFile) BEFORE any os.environ reads below, so every entry
# point resolves the same ${KEYED_RPC_KEY} / ${KEYED_RPC_KEY_2} / ${SOLANA_PROVIDER_KEY} RPC
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

# Minimum free disk space (bytes) — 1GB
MIN_FREE_DISK_BYTES = 1_000_000_000
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

# Bounded worker count for offloading RPC JSON decode off the event loop.
# py-spy sampling of the live daemon (2026-09-13) showed the single event
# loop thread holding the GIL ~63% of wall time, with JSON decode done
# inline; a burst of eth_getLogs responses could stall the loop for >100ms.
# Decode now runs in a dedicated, bounded pool so one chain's decode no
# longer blocks the other chains' network/state work. Kept small: decode is
# not the loop's dominant cost and more threads only add GIL contention.
JSON_DECODE_THREADS: int = max(1, int(os.environ.get("JSON_DECODE_THREADS", "4")))

# Bounded worker count for offloading synchronous ClickHouse inserts /
# row-building off the event loop. py-spy sampling of the live daemon
# (2026-09-13) showed synchronous ClickHouse serialization (~53% of GIL
# samples) plus address-activity row-building (~43%) running inline on the
# event-loop thread — one chain's insert stalled every other chain. Writes
# now run in a dedicated, bounded pool (one ClickHouse connection per
# worker thread via thread-local storage). Kept small: ClickHouse is the
# shared bottleneck, so more concurrency only grows the connection count.
CH_WRITE_THREADS: int = max(1, int(os.environ.get("CH_WRITE_THREADS", "4")))

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
# When the indexer's guarded memory exceeds MEMORY_HIGH_MB the daemon pauses
# NEW chain ingestion (no new sync cycles) until it drops back under the
# threshold. This is an in-process backstop on top of the systemd
# MemoryHigh/MemoryMax cgroup limits: it avoids the OOM-kill churn that
# 14 parallel chains caused on 2026-08-03 by backing off *before* the
# cgroup reaper fires.
#
# METRIC (2026-09-13 post-mortem): the kernel cgroup OOM killer fires on
# cgroup charge (memory.current = anon + file cache + kernel + sock charged
# to the cgroup), NOT process RSS. On 2026-09-13 07:24:51 the daemon was
# OOM-killed with anon-RSS at 5.34 GiB — below the old 6 GiB RSS gate —
# while the cgroup crossed the 7 GiB MemoryMax. The gate now guards
# max(process RSS, cgroup memory.current) so it closes on whichever metric
# is higher. Default 4096 = 4 GiB: real headroom under the 6 GiB
# MemoryHigh drop-in, since cgroup overhead (page cache, kernel, sock)
# exceeds the ~0.9 GiB nominal gap the old 6144 default left. RSS is read
# from /proc/self/statm; cgroup memory from /sys/fs/cgroup/<path>/
# memory.current (auto-detected via /proc/self/cgroup, or CGROUP_MEMORY_PATH).
MEMORY_HIGH_MB: int = int(os.environ.get("MEMORY_HIGH_MB", "4096"))

# Explicit path to a cgroup v2 memory.current file. Empty = auto-detect
# from /proc/self/cgroup. Override when the process runs in a cgroup the
# auto-detection cannot resolve (e.g. nested delegation).
CGROUP_MEMORY_PATH: str = os.environ.get("CGROUP_MEMORY_PATH", "")

# How often (seconds) the memory watchdog may re-read RSS. Reading
# /proc every cycle is cheap, but under heavy GC churn the value flaps;
# caching it bounds the backoff logic.
MEMORY_CHECK_INTERVAL: int = int(os.environ.get("MEMORY_CHECK_INTERVAL", "15"))

# How long (seconds) a chain loop sleeps when the memory gate is closed
# (RSS over MEMORY_HIGH_MB) before re-checking.
MEMORY_PAUSE_SLEEP: int = int(os.environ.get("MEMORY_PAUSE_SLEEP", "30"))

# ── Hard Memory Ceiling: self-recycle ────────────────────────────────────
# Measured 2026-09-13: the daemon RSS oscillated 2.5-3.1 GiB and climbed to
# 5.34 GiB under load, where the kernel cgroup OOM killer fired (7 GiB
# MemoryMax). The pause gate above only *throttles* new ingestion — it does
# not reclaim the live heap, so an unbounded long-running process can still
# ratchet upward until it dies. MEMORY_RECYCLE_MB is the hard ceiling: after
# a completed sync cycle (state already saved) the daemon checks its guarded
# memory and, at or above this threshold, logs ``memory_recycle`` and exits
# cleanly so systemd ``Restart=always`` restarts it with a fresh heap.
# Default 3072 = 3 GiB caps max RSS at ~3.1 GiB, well under MEMORY_HIGH_MB.
# Set MEMORY_RECYCLE_MB=0 to disable self-recycling.
MEMORY_RECYCLE_MB: int = int(os.environ.get("MEMORY_RECYCLE_MB", "3072"))

# ── Bounded cache caps ───────────────────────────────────────────────────
# Every long-lived in-memory collection gets an explicit ceiling; see
# lite_indexer/util/bounded.py. Without these the daemon leaks slowly.
# Pair→tokens map persisted to disk (state.PairCache).
PAIR_CACHE_MAX: int = int(os.environ.get("PAIR_CACHE_MAX", "50000"))
# Launchpad mints already buffered (Solana) — dedup only, safe to roll.
LAUNCHPAD_SEEN_MINTS_MAX: int = int(os.environ.get("LAUNCHPAD_SEEN_MINTS_MAX", "10000"))
# ColdStore (chain, start, end) → parquet file listing.
COLD_LIST_CACHE_MAX: int = int(os.environ.get("COLD_LIST_CACHE_MAX", "256"))
# Historical layer TTL/view-probe caches.
HISTORICAL_CACHE_MAX: int = int(os.environ.get("HISTORICAL_CACHE_MAX", "256"))
# On-demand RPC chain-tip cache.
TIP_CACHE_MAX: int = int(os.environ.get("TIP_CACHE_MAX", "256"))
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
# ── Freemium-Only Backfill Queue ────────────────────────────────────────
# Policy: the free/keyless RPC pool is reserved for the live tip. Ranges
# the indexer did NOT index itself (a stalled/disabled chain's backlog, or
# a range a data-reconciliation rewind flagged as incomplete) are filled by
# a bounded, persisted queue that dispatches ONLY keyed/freemium endpoints
# (KeyedProvider/KeyedProvider2/...). Those have their own per-account caps, so backfill
# never burns the free pool's rate budget. Set BACKFILL_ENABLED=0 to turn
# the sidecar off entirely.
BACKFILL_ENABLED: bool = os.environ.get("BACKFILL_ENABLED", "1") == "1"
# Whether the sidecar may DISPATCH (write) backfill chunks. OFF by default:
# the queue safely collects gaps without racing the live sync loop, and the
# write path stays dormant until ClickHouse dedup for backfilled ranges is
# verified. Set BACKFILL_EXECUTOR_ENABLED=1 to activate the EVM executor.
BACKFILL_EXECUTOR_ENABLED: bool = os.environ.get("BACKFILL_EXECUTOR_ENABLED", "0") == "1"
# Whether reconciliation rewinds are enrolled for backfill. OFF by default:
# the transfer tables are plain MergeTree (no per-key dedup), so re-writing a
# rewound range would duplicate rows. True forward gaps (a stalled/disabled
# chain's backlog) are non-overlapping by construction and always safe. Flip
# to 1 only once those tables are ReplacingMergeTree.
BACKFILL_INCLUDE_REWINDS: bool = os.environ.get("BACKFILL_INCLUDE_REWINDS", "0") == "1"
# Max chunks held pending/in-flight at once (bounded memory + disk).
BACKFILL_MAX_QUEUE_DEPTH: int = int(os.environ.get("BACKFILL_MAX_QUEUE_DEPTH", "256"))
# Max backfill workers running concurrently per chain (small: freemium
# accounts are rate-capped and the daemon has a hard memory ceiling).
BACKFILL_MAX_CONCURRENCY: int = int(os.environ.get("BACKFILL_MAX_CONCURRENCY", "1"))
# Max chunks dispatched per sync cycle (keeps the sidecar short + bounded).
BACKFILL_MAX_CHUNKS_PER_CYCLE: int = int(os.environ.get("BACKFILL_MAX_CHUNKS_PER_CYCLE", "2"))
# Attempts before a failing chunk is parked (retained, not retried) —
# failures never drop the chunk, they just stop it spinning.
BACKFILL_MAX_ATTEMPTS: int = int(os.environ.get("BACKFILL_MAX_ATTEMPTS", "5"))
# Completed chunk keys retained as the idempotency gate (LRU-pruned).
BACKFILL_DONE_RETENTION: int = int(os.environ.get("BACKFILL_DONE_RETENTION", "4096"))
# ── Quota-window-aware backfill scheduling ─────────────────────────────
# Free/freemium tiers reset on a window and expose remaining budget in the
# quota ledger. When enabled, a chunk whose target freemium account is near
# its window cap is DEFERRED (left queued) until just after the window reset
# instead of being dispatched into a 429. Providers with no ledger window
# info are never deferred (no starvation); backfill stays freemium-only and
# lower priority than the live tip.
BACKFILL_WINDOW_AWARE: bool = os.environ.get("BACKFILL_WINDOW_AWARE", "1") == "1"
# Minimum remaining budget (percent of the window) required to dispatch.
# Below this the chunk waits for reset. 20 = spend only when >=20% remains.
BACKFILL_WINDOW_MIN_REMAINING: float = float(os.environ.get("BACKFILL_WINDOW_MIN_REMAINING", "20.0"))
# Upper bound (seconds) on the per-chain jitter added to a post-reset
# dispatch time so every chain does not fire at the same window boundary.
BACKFILL_WINDOW_JITTER_S: float = float(os.environ.get("BACKFILL_WINDOW_JITTER_S", "30.0"))
# Window length (seconds) inferred when the ledger has no reset window.
BACKFILL_WINDOW_INFER_S: float = float(os.environ.get("BACKFILL_WINDOW_INFER_S", "60.0"))
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

# Max token addresses batched per provider request (free-tier URL length).
PRICING_BATCH_SIZE: int = int(os.environ.get("PRICING_BATCH_SIZE", "50"))

# Cap on distinct tokens fetched per refresh cycle (bounds API load on
# the free tiers; the highest-traffic tokens are sampled first).
PRICING_MAX_TRACKED_TOKENS: int = int(os.environ.get("PRICING_MAX_TRACKED_TOKENS", "2500"))

# How far back to scan dex_events for "recently seen" tokens.
PRICING_TOKEN_WINDOW_DAYS: int = int(os.environ.get("PRICING_TOKEN_WINDOW_DAYS", "7"))
# ── Vector Ingestion (LanceDB) ──────────────────────────────────────────
# Optional background pipeline that turns freshly indexed ClickHouse rows
# into semantic embeddings (see lite_indexer/vectors/ingestion.py): it reads
# the last synced block range from ClickHouse, generates text representations
# for wallets and contracts, embeds them via Qwen3-Embedding-4B, and
# stores them in LanceDB collections. OFF by default (OPT-IN) to keep the
# indexer "lite"/zero-cost: the durable store is ClickHouse, not LanceDB, and
# embeddings may use a paid lane. Enable with VECTOR_INGESTION_ENABLED=1.
VECTOR_INGESTION_ENABLED: bool = os.environ.get("VECTOR_INGESTION_ENABLED", "0") == "1"

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
