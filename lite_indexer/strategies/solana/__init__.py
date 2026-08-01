"""
Solana strategy — SPL token transfer indexing via getSignaturesForAddress.
"""

from .models import (
    MAX_SIGNATURES_PER_TOKEN,
    SOLANA_TRACKED_TOKENS,
    SOLANA_TX_CONCURRENCY,
)
from .parsing import parse_solana_transfer_events
from .rpc import SOLANA_ENDPOINTS, rpc_call
from .strategy import SolanaStrategy

# Backward-compat alias for existing importers of _parse_solana_transfer_events
_parse_solana_transfer_events = parse_solana_transfer_events

__all__ = [
    "SolanaStrategy",
    "parse_solana_transfer_events",
    "_parse_solana_transfer_events",
    "rpc_call",
    "SOLANA_ENDPOINTS",
    "SOLANA_TRACKED_TOKENS",
    "MAX_SIGNATURES_PER_TOKEN",
    "SOLANA_TX_CONCURRENCY",
]
