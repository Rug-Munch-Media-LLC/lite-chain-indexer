"""
Tron strategy — TRC20 transfer indexing via TronGrid event API.

Provides the TronStrategy orchestrator class plus focused modules for
configuration (models), event parsing (parsing), and TronGrid RPC
helpers (rpc).
"""

from lite_indexer.chains.non_evm import TRON_ENDPOINTS

from .models import TRC20_TOKENS, TRON_MAX_BLOCK_RANGE, TRON_PAGE_SIZE
from .parsing import _parse_tron_transfer_event, parse_tron_transfer_event
from .rpc import tron_get, tron_get_with_range, tron_post
from .strategy import TronStrategy

__all__ = [
    "TronStrategy",
    "parse_tron_transfer_event",
    "_parse_tron_transfer_event",
    "TRON_ENDPOINTS",
    "TRC20_TOKENS",
    "TRON_PAGE_SIZE",
    "TRON_MAX_BLOCK_RANGE",
    "tron_get",
    "tron_get_with_range",
    "tron_post",
]
