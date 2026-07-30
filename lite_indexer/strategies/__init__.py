"""
Strategy pattern for multi-chain indexing.

Each chain maps to a strategy class based on its chain type:
  - evm    → EVMChainStrategy (eth_getLogs for ERC-20 transfers)
  - solana → SolanaStrategy (getSignaturesForAddress + getTransaction)
  - tron   → TronStrategy (TronGrid event API for TRC20 transfers)

Usage:
    strategy = create_strategy("solana")
    slot = await strategy.fetch_block_number()
    events = await strategy.fetch_transfers(from_slot, to_slot)
"""

from lite_indexer.strategies.base import AbstractChainStrategy
from lite_indexer.strategies.evm import EVMChainStrategy
from lite_indexer.strategies.solana import SolanaStrategy
from lite_indexer.strategies.tron import TronStrategy

# ── Chain Type Mapping ───────────────────────────────────────────────
# Maps chain name → strategy type metadata.
# EVM chains (14): ethereum, base, bsc, polygon, arbitrum, optimism,
#                  avalanche, fantom, linea, scroll, zksync, mantle,
#                  gnosis, celo
# Non-EVM chains (2): solana, tron

SUPPORTED_CHAINS: dict[str, dict[str, str]] = {
    # EVM chains (use GET_LOGS_ENDPOINTS from chains.py)
    "ethereum": {"type": "evm", "label": "Ethereum"},
    "base": {"type": "evm", "label": "Base"},
    "bsc": {"type": "evm", "label": "BSC"},
    "polygon": {"type": "evm", "label": "Polygon"},
    "arbitrum": {"type": "evm", "label": "Arbitrum"},
    "optimism": {"type": "evm", "label": "Optimism"},
    "avalanche": {"type": "evm", "label": "Avalanche C-Chain"},
    "fantom": {"type": "evm", "label": "Fantom"},
    "linea": {"type": "evm", "label": "Linea"},
    "scroll": {"type": "evm", "label": "Scroll"},
    "zksync": {"type": "evm", "label": "zkSync Era"},
    "mantle": {"type": "evm", "label": "Mantle"},
    "gnosis": {"type": "evm", "label": "Gnosis"},
    "celo": {"type": "evm", "label": "Celo"},
    # Non-EVM chains
    "solana": {"type": "solana", "label": "Solana"},
    "tron": {"type": "tron", "label": "Tron"},
}


def create_strategy(
    chain: str,
    rpc_client=None,
    batch_size: int = 1000,
) -> AbstractChainStrategy:
    """Factory: create the appropriate strategy for a chain.

    Args:
        chain: Chain name (e.g., 'ethereum', 'solana', 'tron').
        rpc_client: Optional RpcClient instance (used by EVM strategy).
        batch_size: Max blocks per batch.

    Returns:
        Strategy instance conforming to AbstractChainStrategy.

    Raises:
        ValueError: If chain is not in SUPPORTED_CHAINS.
    """
    info = SUPPORTED_CHAINS.get(chain)
    if info is None:
        raise ValueError(f"Unsupported chain: {chain}. Supported: {', '.join(sorted(SUPPORTED_CHAINS))}")

    chain_type = info["type"]
    if chain_type == "solana":
        return SolanaStrategy(chain=chain)
    elif chain_type == "tron":
        return TronStrategy(chain=chain)
    else:
        return EVMChainStrategy(chain=chain, rpc=rpc_client, batch_size=batch_size)


__all__ = [
    "AbstractChainStrategy",
    "EVMChainStrategy",
    "SolanaStrategy",
    "TronStrategy",
    "SUPPORTED_CHAINS",
    "create_strategy",
]
