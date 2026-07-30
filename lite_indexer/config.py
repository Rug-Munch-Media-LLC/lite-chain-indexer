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
PAIR_CREATED_TOPIC = "0x79b58832c15b09feb3df60e6992ac2bf90bb88a193b76dcf74a5529cfc6a96e2"

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

# ── Known DEX Factory Addresses ───────────────────────────────────────

FACTORY_ADDRESSES: dict[str, list[str]] = {
    "ethereum": ["0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"],  # Uniswap V2
    "base": ["0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6"],
    "bsc": ["0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"],  # PancakeSwap
    "polygon": ["0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32"],  # QuickSwap
    "arbitrum": ["0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9"],
}

V3_FACTORY_ADDRESSES: dict[str, list[str]] = {
    "ethereum": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],  # Uniswap V3
    "base": ["0x33128a8fC17869897dcE68Ed026d694621f6FDfD"],
    "bsc": ["0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7"],  # PancakeSwap V3
    "arbitrum": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
    "optimism": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
    "polygon": ["0x1F98431c8aD98523631AE4a59f267346ea31F984"],
}

# Alias for backward-compatibility in indexer imports
CHAIN_INDEXER_DIR = DATA_DIR

# ── Solana Configuration ──────────────────────────────────────────────

SOLANA_RPC_ENDPOINTS: list[str] = [
    "https://api.mainnet-beta.solana.com",
    "https://rpc.ankr.com/solana",
    "https://solana.drpc.org",
]

SOLANA_SPL_TOKEN_PROGRAM: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

SOLANA_MAX_SIGNATURES_PER_TOKEN: int = 100
SOLANA_TX_CONCURRENCY: int = 10

SOLANA_CH_TABLE: str = "wallet_memory.solana_spl_balance_changes"

# ── Tron Configuration ────────────────────────────────────────────────

TRON_RPC_ENDPOINTS: list[str] = [
    "https://api.trongrid.io",
    "https://rpc.tronlink.org",
    "https://api.tronstack.io",
]

TRON_PAGE_SIZE: int = 200
TRON_MAX_BLOCK_RANGE: int = 2000

TRC20_CH_TABLE: str = "wallet_memory.evm_transfers"
