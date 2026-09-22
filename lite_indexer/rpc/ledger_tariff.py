"""Quota tariff table + degrade ladder (shared constants for the ledger).

Extracted from ``rpc/ledger.py``: the per-method tariff weights, the
degrade-ladder thresholds, and the level-2/level-1 defer sets. Pure
constants + two pure helpers — no I/O, no state.
"""

from __future__ import annotations

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Unit types and periods understood by QuotaBudget.
UNIT_TYPES = ("CU", "credits", "request_units", "raw")
PERIODS = ("day", "30d", "month")

# One base tariff unit ≈ 5 CU (matches the documented eth_chainId @ 5 CU and
# eth_getLogs @ 75 CU example — a 15x spread). Used to scale the relative
# tariff table for CU/credits budgets; request-unit budgets use the raw
# relative weights.
CU_PER_UNIT: float = 5.0

# Relative per-method tariff weights. eth_getLogs is 15x eth_chainId. Tron
# method names (wallet/...) are included for request-unit providers.
BASE_TARIFF_UNITS: dict[str, float] = {
    # Cheap, high-frequency calls (1 unit).
    "eth_chainId": 1.0,
    "eth_blockNumber": 1.0,
    "eth_syncing": 1.0,
    "eth_gasPrice": 1.0,
    "eth_getBalance": 1.0,
    "eth_getTransactionCount": 1.0,
    "eth_getCode": 1.0,
    "net_version": 1.0,
    "web3_clientVersion": 1.0,
    "wallet/getnowblock": 1.0,
    "wallet/getblock": 1.0,
    # Moderate (2-4 units).
    "eth_getBlockByNumber": 2.0,
    "eth_getBlockByHash": 2.0,
    "eth_getTransactionByHash": 2.0,
    "eth_getTransactionReceipt": 2.0,
    "eth_getBlockReceipts": 4.0,
    "wallet/gettransactionbyid": 2.0,
    "wallet/getblockbylimitnext": 2.0,
    # eth_call (3 units) — cheap per call but hot in backfill.
    "eth_call": 3.0,
    "wallet/gettransactioninfobyid": 3.0,
    "wallet/gettriggercontractresult": 3.0,
    # Expensive (15-20 units).
    "eth_getLogs": 15.0,
    "eth_getLogsV2": 15.0,
    "trace_block": 20.0,
    "trace_filter": 20.0,
    "trace_transaction": 20.0,
    "eth_traceTransaction": 20.0,
    # Fallback for anything unlisted.
    "default": 1.0,
}

# Degrade ladder: (consumed_pct threshold, level, action description).
# Level 0 = normal; 3 = key parked until reset_at.
DEGRADE_LADDER: tuple[tuple[float, int, str], ...] = (
    (70.0, 1, "widen head-follow polls + narrow eth_getLogs windows"),
    (85.0, 2, "head-only: no backfill or getLogs"),
    (95.0, 3, "park key until reset_at"),
)

# Methods deferred at level 2 (85% consumed, head-only). ``debug_trace*``
# and ``trace_*`` family members match by prefix via :func:`_is_heavy_method`.
HEAVY_DEFER_METHODS: frozenset[str] = frozenset(
    {
        "eth_getLogs",
        "eth_getLogsV2",
        "eth_getBlockReceipts",
        "getSignaturesForAddress",
        "getBlock",
        "getTransaction",
        "trace_block",
        "trace_filter",
        "trace_transaction",
        "eth_traceTransaction",
    }
)

# Level-1 (70%) wide-scan threshold: ``eth_getLogs`` ranges wider than this
# many blocks are treated as backfill-style wide scans and deferred; narrow
# head-follow windows stay allowed.
LEVEL1_GETLOGS_RANGE_BLOCKS: int = 1000


def degrade_action(level: int) -> str:
    """Human-readable action for a degrade level (0 = normal).

    Returns the action for the HIGHEST threshold met (a parked key reports
    the level-3 action, not the level-1 one).
    """
    for _threshold, lvl, action in reversed(DEGRADE_LADDER):
        if level >= lvl:
            return action
    return "normal operation"


def _is_heavy_method(method: str) -> bool:
    """Whether a method is in the level-2 heavy-defer set (prefix-aware)."""
    return method in HEAVY_DEFER_METHODS or method.startswith("debug_trace")
