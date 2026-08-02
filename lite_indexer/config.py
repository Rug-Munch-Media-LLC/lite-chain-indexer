"""
Configuration constants for LiteChainIndexer.

All tunable parameters live here — RPC timeouts, batch sizes,
disk caps, event topics, and factory addresses.
"""

from __future__ import annotations

import os
from pathlib import Path

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
BLOOM_CACHE_TTL: int = 7 * 24 * 3600  # 7 days

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

# Retention (days) per table before column-level degradation.
RETENTION_DAYS: dict[str, int] = {
    "evm_transfers": 30,
    "dex_events": 90,
    "deployer_events": 365,
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
CIRCUIT_FAILURE_THRESHOLD: int = 5
CIRCUIT_WINDOW_SECONDS: int = 60
CIRCUIT_COOLDOWN_SECONDS: int = 30

# RPC usage/cost tracking: keep a bounded per-endpoint usage ledger.
RPC_USAGE_MAX_ENDPOINTS: int = 512

# RPC auto-discovery: how often (seconds) to scan chainlist.org for new
# free endpoints. 0 disables. Default: daily.
RPC_DISCOVERY_INTERVAL: int = int(os.environ.get("RPC_DISCOVERY_INTERVAL", "86400"))

# ── Multi-Chain Parallelism ───────────────────────────────────────────
# Per-chain CPU affinity (Linux only, best-effort). Used when running a
# single chain per process; the multi-chain daemon uses priorities.
CHAIN_AFFINITY_ENABLED: bool = os.environ.get("CHAIN_AFFINITY_ENABLED", "0") == "1"

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
}

# ── Parquet Cold Storage Tier ─────────────────────────────────────────
# Daily rollup of old ClickHouse rows into Parquet, uploaded to B2, then
# deleted from ClickHouse to reclaim disk. Optional dependency: pyarrow.
PARQUET_DIR: Path = DATA_DIR / "parquet"
PARQUET_REMOTE: str = os.environ.get("PARQUET_REMOTE", "b2-crypt:chain-data/parquet")
PARQUET_RCLONE_BIN: str = os.environ.get("PARQUET_RCLONE_BIN", "rclone")
# Min age (days) of rows eligible for cold storage (keep hot window).
PARQUET_COLD_DAYS: int = int(os.environ.get("PARQUET_COLD_DAYS", "7"))
# State dir records which (chain, date) files were already exported so
# the daily job is idempotent.
PARQUET_STATE_DIR: Path = STATE_DIR / "parquet"

# ── Solana Configuration ──────────────────────────────────────────────

SOLANA_SPL_TOKEN_PROGRAM: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

SOLANA_MAX_SIGNATURES_PER_TOKEN: int = 100
SOLANA_TX_CONCURRENCY: int = 10

SOLANA_CH_TABLE: str = "wallet_memory.solana_spl_balance_changes"

# ── Tron Configuration ────────────────────────────────────────────────

TRON_PAGE_SIZE: int = 200
TRON_MAX_BLOCK_RANGE: int = 2000

TRC20_CH_TABLE: str = "wallet_memory.evm_transfers"
