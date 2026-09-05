"""Chain-specific configuration for LiteChainIndexer."""

from __future__ import annotations

import os

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
# ── Lazy Indexing (per-chain event type gating) ─────────────────────────
# When enabled (default), every chain starts with transfer events only.
# DEX swap and deployer event indexing is enabled by default for top
# chains (see lite_indexer.chains.demand.TOP_CHAINS) and for long-tail
# chains only after the scanner (rmi-backend) signals demand via a file
# touch. Setting LAZY_INDEXING_ENABLED=0 restores the legacy behavior of
# indexing all event types equally (CLI flags are the authority).
LAZY_INDEXING_ENABLED: bool = os.environ.get("LAZY_INDEXING_ENABLED", "1") != "0"
# ── Solana Configuration ──────────────────────────────────────────────

SOLANA_SPL_TOKEN_PROGRAM: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

SOLANA_MAX_SIGNATURES_PER_TOKEN: int = 100
SOLANA_TX_CONCURRENCY: int = 10

SOLANA_CH_TABLE: str = "wallet_memory.solana_spl_balance_changes"

# ── Tron Configuration ────────────────────────────────────────────────

TRON_PAGE_SIZE: int = 200
TRON_MAX_BLOCK_RANGE: int = 2000

TRC20_CH_TABLE: str = "wallet_memory.evm_transfers"
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
