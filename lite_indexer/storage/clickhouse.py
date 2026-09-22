"""
ClickHouse writer — inserts events via ClickHouse native protocol.

Handles lazy connections, column existence checks, and batch inserts
for transfer events, DEX swaps, and deployer events.

Deployer events resolve the real deployer EOA (the transaction `from`
address) via eth_getTransactionByHash before inserting — the PairCreated/
PoolCreated log itself only exposes the factory address, not the EOA that
submitted the transaction.

Column pruning (Improvement 3): old rows keep their summary columns
(from/to/token/value/block_ts for transfers, swap identity for DEX)
while heavy detail columns degrade to type defaults via column-level
TTL — cutting storage 30-50% while keeping every scanner query
answerable.

Split into cohesive modules (2026-09-22, shrink-only ratchet); this
module is the public facade — every historically importable name is
re-exported here, and the implementation reads the config flags tests
patch on this facade at call time.
"""

from __future__ import annotations

import asyncio  # noqa: F401  (module attribute preserved)
import json  # noqa: F401  (module attribute preserved)
import os  # noqa: F401  (module attribute preserved)
import threading  # noqa: F401  (module attribute preserved)
from datetime import UTC, datetime  # noqa: F401  (UTC re-exported)
from typing import Any  # noqa: F401  (module attribute preserved)

from lite_indexer.config import (
    BALANCE_CACHE_ENABLED,  # noqa: F401  (re-exported; tests patch it here)
    CH_DEPLOYER_TABLE,  # noqa: F401  (re-exported)
    CH_DEX_TABLE,  # noqa: F401  (re-exported)
    CH_TABLE,  # noqa: F401  (re-exported)
    CH_TTL_ENABLED,  # noqa: F401  (re-exported; tests patch it here)
    HOLDER_BALANCES_MV_ENABLED,  # noqa: F401  (re-exported; tests patch it here)
    TOKEN_META_ENABLED,  # noqa: F401  (re-exported; tests patch it here)
    WALLET_PROJECTION_ENABLED,  # noqa: F401  (re-exported; tests patch it here)
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr  # noqa: F401  (re-exported)
from lite_indexer.pricing import compute_swap_usd, detect_whale, get_price  # noqa: F401  (re-exported)
from lite_indexer.storage._schema import SchemaMixin
from lite_indexer.storage.balance_cache import (  # noqa: F401  (re-exported)
    ensure_balance_cache_table,
    ensure_holder_balances_mv,
    update_holder_balances_bulk,
)
from lite_indexer.storage.ch_connect import ConnectMixin
from lite_indexer.storage.ch_deployer import DeployerMixin
from lite_indexer.storage.ch_dex import DexMixin
from lite_indexer.storage.ch_errors import (  # noqa: F401  (re-exported)
    ClickHouseUnavailable,
    is_unavailable_error,
    require_client,
)
from lite_indexer.storage.ch_features import FeaturesMixin
from lite_indexer.storage.ch_rows import (  # noqa: F401  (re-exported)
    _DEDUP_MAX_BLOCK_SPAN,
    _DEPLOYER_RPC_CONCURRENCY,
    _DEPLOYER_RPC_MAX_ENDPOINTS,
    _coerce_int,
    _resolve_deployer_eoas,
    _serialize_raw_payload,
    _transfer_dedup_key,
)
from lite_indexer.storage.ch_transfers import TransfersMixin
from lite_indexer.storage.projections import ensure_wallet_projection  # noqa: F401  (re-exported)
from lite_indexer.storage.token_meta import (  # noqa: F401  (re-exported)
    ensure_token_meta_table,
    upsert_token_meta,
)

logger = get_logger(__name__)


class ClickHouseWriter(
    ConnectMixin,
    TransfersMixin,
    DexMixin,
    DeployerMixin,
    FeaturesMixin,
    SchemaMixin,
):
    """Insert events into ClickHouse tables.

    Connects lazily on first write. Handles column existence checks.
    The method bodies live in the ``ch_*`` sibling modules (connect,
    transfers, dex, deployer, features, rows); this class composes them
    and preserves the historical import surface.
    """


__all__ = [
    "ClickHouseWriter",
    "ClickHouseUnavailable",
    "_resolve_deployer_eoas",
    "_coerce_int",
    "_transfer_dedup_key",
    "_serialize_raw_payload",
]
