"""
DEX event decoders — V2/V3 swap parsing, PairCreated/PoolCreated extraction.

Split by concern:
  - common:    shared address-unpadding and log primitives
  - v2:        Uniswap V2 PairCreated and Swap decoders
  - v3:        Uniswap V3 PoolCreated and Swap decoders
  - transfers: ERC-20 transfer event decoder
  - parsers:   top-level dispatch entry points used by strategy handlers

All public names are re-exported here so `from lite_indexer.dex import X`
keeps working after the split.
"""

from lite_indexer.config import (
    MAX_EVENTS_PER_BATCH,
    PAIR_CREATED_TOPIC,
    POOL_CREATED_TOPIC,
    SWAP_V2_TOPIC,
)

from .common import parse_block_number, parse_log_index, unpad_address
from .lp import lp_event_type, parse_lp_log
from .lp_logs import parse_dex_logs, parse_lp_logs
from .parsers import parse_pair_created_logs, parse_swap_logs
from .transfers import parse_transfer_logs
from .v2 import parse_pair_created_v2, parse_swap_v2
from .v3 import parse_pool_created_v3, parse_swap_v3

__all__ = [
    "unpad_address",
    "parse_block_number",
    "parse_log_index",
    "parse_pair_created_logs",
    "parse_dex_logs",
    "parse_lp_logs",
    "parse_lp_log",
    "lp_event_type",
    "parse_swap_logs",
    "parse_transfer_logs",
    "parse_pair_created_v2",
    "parse_swap_v2",
    "parse_pool_created_v3",
    "parse_swap_v3",
    # Re-exported config constants (previously accessible via dex module)
    "MAX_EVENTS_PER_BATCH",
    "PAIR_CREATED_TOPIC",
    "POOL_CREATED_TOPIC",
    "SWAP_V2_TOPIC",
]
