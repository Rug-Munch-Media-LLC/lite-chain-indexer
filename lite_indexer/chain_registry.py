"""
Chain Configuration Registry — plug-and-play chain support.

Makes adding a new chain a matter of dropping a config dict into a YAML/JSON file.
No code changes required for new EVM chains. Built-in presets for 50+ chains.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ChainType(Enum):
    EVM = "evm"
    SOLANA = "solana"
    TRON = "tron"
    COSMOS = "cosmos"
    APTOS = "aptos"
    SUI = "sui"
    BITCOIN = "bitcoin"


@dataclass
class ChainConfig:
    """Complete configuration for indexing a single chain."""

    name: str
    chain_type: ChainType
    chain_id: int = 0
    rpc_endpoints: list[str] = field(default_factory=list)
    poll_interval_seconds: int = 12
    batch_size: int = 2000
    max_concurrent_requests: int = 5
    supports_dex: bool = True
    supports_deployers: bool = True
    supports_traces: bool = True
    dex_factory_addresses: list[str] = field(default_factory=list)
    explorer_api_url: str = ""
    explorer_api_key_env: str = ""
    #: Optional dedicated RPC URL template for a specific provider (e.g. the
    #: use time; an unset key leaves the endpoint inactive.
    provider_rpc_url: str = ""
    native_currency: str = "ETH"
    block_time_seconds: float = 12.0
    max_reorg_depth: int = 20
    notes: str = ""


# ── Preset chain configurations ──────────────────────────────────────────

EVM_PRESETS: dict[str, ChainConfig] = {
    "ethereum": ChainConfig(
        name="ethereum",
        chain_type=ChainType.EVM,
        chain_id=1,
        rpc_endpoints=[
            "https://eth.llamarpc.com",
            "https://ethereum.publicnode.com",
        ],
        dex_factory_addresses=[
            "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",  # Uniswap V2
            "0x1F98431c8aD98523631AE4a59f267346ea31F984",  # Uniswap V3
            "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",  # PancakeSwap V2
        ],
        native_currency="ETH",
        block_time_seconds=12.0,
    ),
    "base": ChainConfig(
        name="base",
        chain_type=ChainType.EVM,
        chain_id=8453,
        rpc_endpoints=[
            "https://mainnet.base.org",
            "https://base.llamarpc.com",
        ],
        dex_factory_addresses=[
            "0x8909Dc15e40173Ff643900413796596626800331",  # Uniswap V2
            "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",  # Uniswap V3
        ],
        native_currency="ETH",
        block_time_seconds=2.0,
    ),
    "bsc": ChainConfig(
        name="bsc",
        chain_type=ChainType.EVM,
        chain_id=56,
        rpc_endpoints=[
            "https://bsc-dataseed.binance.org",
            "https://bsc.publicnode.com",
        ],
        dex_factory_addresses=[
            "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",  # PancakeSwap V2
            "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",  # PancakeSwap V3
        ],
        native_currency="BNB",
        block_time_seconds=3.0,
    ),
    "polygon": ChainConfig(
        name="polygon",
        chain_type=ChainType.EVM,
        chain_id=137,
        rpc_endpoints=[
            "https://polygon-rpc.com",
            "https://polygon.llamarpc.com",
        ],
        native_currency="MATIC",
        block_time_seconds=2.0,
    ),
    "arbitrum": ChainConfig(
        name="arbitrum",
        chain_type=ChainType.EVM,
        chain_id=42161,
        rpc_endpoints=[
            "https://arb1.arbitrum.io/rpc",
            "https://arbitrum.llamarpc.com",
        ],
        native_currency="ETH",
        block_time_seconds=0.25,
    ),
    "optimism": ChainConfig(
        name="optimism",
        chain_type=ChainType.EVM,
        chain_id=10,
        rpc_endpoints=[
            "https://mainnet.optimism.io",
            "https://optimism.llamarpc.com",
        ],
        native_currency="ETH",
        block_time_seconds=2.0,
    ),
    "avalanche": ChainConfig(
        name="avalanche",
        chain_type=ChainType.EVM,
        chain_id=43114,
        rpc_endpoints=[
            "https://api.avax.network/ext/bc/C/rpc",
            "https://avalanche.publicnode.com",
        ],
        native_currency="AVAX",
        block_time_seconds=2.0,
    ),
    "fantom": ChainConfig(
        name="fantom",
        chain_type=ChainType.EVM,
        chain_id=250,
        rpc_endpoints=[
            "https://rpc.ftm.tools",
            "https://fantom.publicnode.com",
        ],
        native_currency="FTM",
        block_time_seconds=1.0,
    ),
    "linea": ChainConfig(
        name="linea",
        chain_type=ChainType.EVM,
        chain_id=59144,
        rpc_endpoints=[
            "https://rpc.linea.build",
        ],
        native_currency="ETH",
        block_time_seconds=2.0,
    ),
    "scroll": ChainConfig(
        name="scroll",
        chain_type=ChainType.EVM,
        chain_id=534352,
        rpc_endpoints=[
            "https://rpc.scroll.io",
        ],
        native_currency="ETH",
        block_time_seconds=3.0,
    ),
    "zksync": ChainConfig(
        name="zksync",
        chain_type=ChainType.EVM,
        chain_id=324,
        rpc_endpoints=[
            "https://mainnet.era.zksync.io",
        ],
        native_currency="ETH",
        block_time_seconds=2.0,
    ),
    "mantle": ChainConfig(
        name="mantle",
        chain_type=ChainType.EVM,
        chain_id=5000,
        rpc_endpoints=[
            "https://rpc.mantle.xyz",
        ],
        native_currency="MNT",
        block_time_seconds=2.0,
    ),
    "gnosis": ChainConfig(
        name="gnosis",
        chain_type=ChainType.EVM,
        chain_id=100,
        rpc_endpoints=[
            "https://rpc.gnosischain.com",
            "https://gnosis.publicnode.com",
        ],
        native_currency="XDAI",
        block_time_seconds=5.0,
    ),
    "celo": ChainConfig(
        name="celo",
        chain_type=ChainType.EVM,
        chain_id=42220,
        rpc_endpoints=[
            "https://forno.celo.org",
        ],
        native_currency="CELO",
        block_time_seconds=5.0,
    ),
    "robinhood": ChainConfig(
        name="robinhood",
        chain_type=ChainType.EVM,
        chain_id=4663,
        rpc_endpoints=[
            "https://rpc.mainnet.chain.robinhood.com",
            "https://robinhood-rpc.publicnode.com",
        ],
        native_currency="ETH",
        block_time_seconds=0.5,
    ),
    "hyperliquid": ChainConfig(
        name="hyperliquid",
        chain_type=ChainType.EVM,
        chain_id=999,
        rpc_endpoints=[
            "https://rpc.hyperliquid.xyz/evm",
        ],
        native_currency="HYPE",
        block_time_seconds=1.0,
    ),
    "solana": ChainConfig(
        name="solana",
        chain_type=ChainType.SOLANA,
        chain_id=0,
        rpc_endpoints=[
            "https://api.mainnet-beta.solana.com",
            "https://solana.publicnode.com",
        ],
        # set (gopass [INTERNAL_SECRET_PATH] → daemon env file). Used by the
        poll_interval_seconds=5,
        native_currency="SOL",
        block_time_seconds=0.4,
    ),
    "tron": ChainConfig(
        name="tron",
        chain_type=ChainType.TRON,
        chain_id=0,
        rpc_endpoints=[
            "https://api.trongrid.io",
        ],
        poll_interval_seconds=3,
        native_currency="TRX",
        block_time_seconds=3.0,
    ),
}


def get_chain_config(name: str) -> ChainConfig | None:
    """Get chain configuration by name."""
    return EVM_PRESETS.get(name.lower())


def list_supported_chains() -> list[str]:
    """List all supported chain names."""
    return list(EVM_PRESETS.keys())


def add_chain_config(name: str, config: ChainConfig) -> None:
    """Add a new chain configuration at runtime."""
    EVM_PRESETS[name.lower()] = config
